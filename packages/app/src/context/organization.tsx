import {
  createContext,
  createEffect,
  createResource,
  createSignal,
  type ParentProps,
  Show,
  useContext,
} from "solid-js"
import { OrganizationOnboarding } from "@/pages/organization-onboarding"
import { OrganizationSwitcher } from "@/components/organization-switcher"

export type OrganizationRole = "owner" | "admin" | "member" | "viewer" | "billing_admin"
export type OrganizationPermissionKey =
  | "organization.view"
  | "organization.members.view"
  | "organization.members.invite"
  | "organization.members.manage"
  | "organization.permissions.view"
  | "organization.permissions.manage"
  | "organization.owner.transfer"
  | "project.view"
  | "project.manage"
  | "session.view"
  | "session.create"
  | "session.manage"
  | "agent_run.execute"
  | "agent_run.stop"
  | "audit.view"
  | "billing.view"
  | "billing.manage"

export type OrganizationPermissionPolicy = {
  readonly organizationID: string
  readonly version: number
  readonly catalog: readonly {
    readonly key: OrganizationPermissionKey
    readonly group: string
    readonly label: string
    readonly description: string
    readonly delegable: boolean
  }[]
  readonly roles: readonly {
    readonly role: OrganizationRole
    readonly permissions: readonly OrganizationPermissionKey[]
    readonly mutable: boolean
  }[]
  readonly actor: {
    readonly actorID: string
    readonly role: OrganizationRole
    readonly permissions: readonly OrganizationPermissionKey[]
  }
}

export type OrganizationSummary = {
  readonly id: string
  readonly slug: string
  readonly name: string
  readonly status: "active" | "suspended" | "deleted"
  readonly role: OrganizationRole
  readonly timeCreated: number
  readonly timeUpdated: number
}

export type OrganizationMember = {
  readonly actorID: string
  readonly email: string
  readonly name: string
  readonly role: OrganizationRole
  readonly status: "invited" | "active" | "suspended"
  readonly timeCreated: number
  readonly timeUpdated: number
}

export type OrganizationInvitation = {
  readonly id: string
  readonly organizationID: string
  readonly email: string
  readonly role: Exclude<OrganizationRole, "owner">
  readonly invitedBy: string
  readonly status: "pending" | "accepted" | "rejected" | "revoked" | "expired"
  readonly expiresAt: number
  readonly timeCreated: number
  readonly timeUpdated: number
}

type LoadState =
  | { readonly mode: "disabled" }
  | { readonly mode: "enabled"; readonly organizations: readonly OrganizationSummary[] }

type OrganizationContextValue = {
  readonly organizations: () => readonly OrganizationSummary[]
  readonly selected: () => OrganizationSummary | undefined
  readonly select: (organizationID: string) => void
  readonly permissionPolicy: () => OrganizationPermissionPolicy | undefined
  readonly can: (permission: OrganizationPermissionKey) => boolean
  readonly refresh: () => Promise<unknown>
  readonly refreshPermissions: () => Promise<unknown>
  readonly beginCreate: () => void
}

const OrganizationContext = createContext<OrganizationContextValue>()
const STORAGE_KEY = "opencode.saas.organization"

export function useOrganization() {
  const value = useContext(OrganizationContext)
  if (value === undefined) throw new Error("useOrganization must be used within OrganizationGate")
  return value
}

type OrganizationFetcher = (
  path: string,
  init?: RequestInit,
  allowDisabled?: boolean,
) => Promise<readonly OrganizationSummary[] | undefined>

export async function readOrganizations(fetcher: OrganizationFetcher = organizationRequest): Promise<LoadState> {
  const response = await fetcher("/api/organizations", undefined, true)
  if (response === undefined) return { mode: "disabled" }
  return { mode: "enabled", organizations: response }
}

export async function organizationRequest<T>(
  path: string,
  init?: RequestInit,
  allowDisabled = false,
): Promise<T | undefined> {
  const response = await fetch(path, {
    ...init,
    credentials: "include",
    headers: {
      accept: "application/json",
      ...(init?.body ? { "content-type": "application/json" } : {}),
      ...init?.headers,
    },
  })
  if (allowDisabled && response.status === 404) return undefined
  if (!response.ok) {
    const body = (await response.json().catch(() => undefined)) as { message?: string; error?: string } | undefined
    throw new Error(body?.message ?? body?.error ?? `Organization request failed with ${response.status}`)
  }
  return (await response.json()) as T
}

export function organizationSlugFromName(name: string) {
  return name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 63)
}

export function hasOrganizationPermission(
  policy: OrganizationPermissionPolicy | undefined,
  permission: OrganizationPermissionKey,
) {
  return policy?.actor.permissions.includes(permission) ?? false
}

export function OrganizationGate(props: ParentProps) {
  const [data, actions] = createResource(() => readOrganizations())
  const [selectedID, setSelectedID] = createSignal(readStoredOrganization())
  const [creating, setCreating] = createSignal(false)
  const invitationToken = typeof location === "undefined" ? undefined : new URLSearchParams(location.search).get("invitation")
  const [invitation] = createResource(
    () => invitationToken,
    async (token) => {
      await organizationRequest("/api/organization-invitations/accept", {
        method: "POST",
        body: JSON.stringify({ token }),
      })
      const url = new URL(location.href)
      url.searchParams.delete("invitation")
      history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`)
      await actions.refetch()
      return true
    },
  )

  const organizations = (): readonly OrganizationSummary[] => {
    const value = data()
    return value?.mode === "enabled" ? value.organizations : []
  }
  const selected = () =>
    organizations().find((organization) => organization.id === selectedID()) ?? organizations()[0]
  const [permissionPolicy, permissionActions] = createResource(
    () => selected()?.id,
    (organizationID) =>
      organizationRequest<OrganizationPermissionPolicy>(
        `/api/organizations/${organizationID}/permissions`,
      ),
  )

  createEffect(() => {
    const organization = selected()
    if (organization === undefined) return
    if (organization.id !== selectedID()) setSelectedID(organization.id)
    localStorage.setItem(STORAGE_KEY, organization.id)
  })

  const select = (organizationID: string) => {
    if (!organizations().some((organization) => organization.id === organizationID)) return
    setSelectedID(organizationID)
    localStorage.setItem(STORAGE_KEY, organizationID)
  }

  const create = async (input: { readonly name: string; readonly slug: string }) => {
    const organization = await organizationRequest<OrganizationSummary>("/api/organizations", {
      method: "POST",
      body: JSON.stringify(input),
    })
    if (organization === undefined) return
    await actions.refetch()
    setSelectedID(organization.id)
    localStorage.setItem(STORAGE_KEY, organization.id)
    setCreating(false)
  }

  const context: OrganizationContextValue = {
    organizations,
    selected,
    select,
    permissionPolicy,
    can: (permission) => hasOrganizationPermission(permissionPolicy(), permission),
    refresh: async () => {
      await Promise.all([actions.refetch(), permissionActions.refetch()])
    },
    refreshPermissions: async () => {
      await permissionActions.refetch()
    },
    beginCreate: () => setCreating(true),
  }

  const loading = () => (data.loading && data() === undefined) || invitation.loading
  const error = () => {
    const value = invitation.error ?? data.error
    return value instanceof Error ? value.message : undefined
  }

  return (
    <>
      <Show when={!loading()} fallback={<OrganizationLoading />}>
        <Show when={data()?.mode === "disabled"} fallback={
          <OrganizationContext.Provider value={context}>
            <Show
              when={!creating() && selected()}
              fallback={
                <OrganizationOnboarding
                  canCancel={organizations().length > 0}
                  error={error()}
                  onCancel={() => setCreating(false)}
                  onCreate={create}
                />
              }
            >
              <OrganizationSwitcher />
              {props.children}
            </Show>
          </OrganizationContext.Provider>
        }>
          {props.children}
        </Show>
      </Show>
    </>
  )
}

function readStoredOrganization() {
  if (typeof localStorage === "undefined") return undefined
  return localStorage.getItem(STORAGE_KEY) ?? undefined
}

function OrganizationLoading() {
  return (
    <div class="organization-loading">
      <span />
      Resolving organization context
    </div>
  )
}
