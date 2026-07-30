import { createMemo, createResource, createSignal, For, Show } from "solid-js"
import {
  organizationRequest,
  type OrganizationInvitation,
  type OrganizationMember,
  type OrganizationRole,
  useOrganization,
} from "@/context/organization"
import { signOutAuthSession, useSaasAuth } from "@/context/saas-auth"
import { OrganizationPermissionCenter } from "@/components/organization-permission-center"
import "@/pages/organization-control.css"

type EditableRole = Exclude<OrganizationRole, "owner">

export function OrganizationSwitcher() {
  const organization = useOrganization()
  const auth = useSaasAuth()
  const [open, setOpen] = createSignal(false)
  const [inviteEmail, setInviteEmail] = createSignal("")
  const [inviteRole, setInviteRole] = createSignal<EditableRole>("member")
  const [inviteLink, setInviteLink] = createSignal<string>()
  const [working, setWorking] = createSignal(false)
  const [error, setError] = createSignal<string>()

  const [members, memberActions] = createResource(
    () => (open() && organization.can("organization.members.view") ? organization.selected()?.id : undefined),
    (organizationID) =>
      organizationRequest<readonly OrganizationMember[]>(`/api/organizations/${organizationID}/members`).then(
        (value) => value ?? [],
      ),
  )
  const [invitations, invitationActions] = createResource(
    () => (open() && organization.can("organization.members.invite") ? organization.selected()?.id : undefined),
    (organizationID) =>
      organizationRequest<readonly OrganizationInvitation[]>(`/api/organizations/${organizationID}/invitations`).then(
        (value) => value ?? [],
      ),
  )
  const actorID = createMemo(() => {
    const state = auth.state()
    return state.mode === "authenticated" ? state.session.user.id : undefined
  })

  const run = async (operation: () => Promise<unknown>, refresh = true) => {
    setWorking(true)
    setError(undefined)
    await operation()
      .then(async () => {
        if (refresh) await Promise.all([memberActions.refetch(), invitationActions.refetch(), organization.refresh()])
      })
      .catch((cause) => setError(cause instanceof Error ? cause.message : "Organization operation failed"))
    setWorking(false)
  }

  const invite = async (event: SubmitEvent) => {
    event.preventDefault()
    const selected = organization.selected()
    if (selected === undefined) return
    await run(async () => {
      const result = await organizationRequest<{ readonly token: string }>(
        `/api/organizations/${selected.id}/invitations`,
        {
          method: "POST",
          body: JSON.stringify({ email: inviteEmail().trim(), role: inviteRole() }),
        },
      )
      if (result === undefined) return
      const url = new URL("/", location.origin)
      url.searchParams.set("invitation", result.token)
      setInviteLink(url.toString())
      setInviteEmail("")
    })
  }

  const updateRole = (member: OrganizationMember, role: EditableRole) => {
    const selected = organization.selected()
    if (selected === undefined) return
    return run(() =>
      organizationRequest(`/api/organizations/${selected.id}/members/${member.actorID}`, {
        method: "PATCH",
        body: JSON.stringify({ role }),
      }),
    )
  }

  const remove = (member: OrganizationMember) => {
    const selected = organization.selected()
    if (selected === undefined || !window.confirm(`Remove ${member.name} from ${selected.name}?`)) return
    void run(() =>
      organizationRequest(`/api/organizations/${selected.id}/members/${member.actorID}`, { method: "DELETE" }),
    )
  }

  const transfer = (member: OrganizationMember) => {
    const selected = organization.selected()
    if (
      selected === undefined ||
      !window.confirm(`Transfer ownership of ${selected.name} to ${member.name}? Your role will become admin.`)
    ) {
      return
    }
    void run(() =>
      organizationRequest(`/api/organizations/${selected.id}/transfer-ownership`, {
        method: "POST",
        body: JSON.stringify({ actorID: member.actorID }),
      }),
    )
  }

  const signOut = async () => {
    setWorking(true)
    setError(undefined)
    await signOutAuthSession()
      .then(() => location.reload())
      .catch((cause) => {
        setError(cause instanceof Error ? cause.message : "Sign out failed")
        setWorking(false)
      })
  }

  return (
    <>
      <button class="organization-trigger" type="button" onClick={() => setOpen(true)}>
        <span class="organization-trigger__glyph">{organization.selected()?.name.slice(0, 2).toUpperCase()}</span>
        <span>
          <small>ORGANIZATION</small>
          <strong>{organization.selected()?.name}</strong>
        </span>
        <span class="organization-trigger__chevron">⌄</span>
      </button>

      <Show when={open()}>
        <div class="organization-drawer__scrim" onClick={() => setOpen(false)} />
        <aside class="organization-drawer" aria-label="Organization center">
          <header>
            <div>
              <p class="organization-kicker">Organization Center</p>
              <h2>{organization.selected()?.name}</h2>
            </div>
            <button class="organization-icon-button" type="button" aria-label="Close" onClick={() => setOpen(false)}>
              ×
            </button>
          </header>

          <section class="organization-drawer__selector">
            <label>
              Active organization
              <select
                value={organization.selected()?.id}
                onChange={(event) => organization.select(event.currentTarget.value)}
              >
                <For each={organization.organizations()}>
                  {(item) => <option value={item.id}>{item.name} · {item.role}</option>}
                </For>
              </select>
            </label>
            <button
              class="organization-button organization-button--quiet"
              type="button"
              onClick={() => {
                setOpen(false)
                organization.beginCreate()
              }}
            >
              New organization
            </button>
          </section>

          <section class="organization-drawer__section">
            <div class="organization-drawer__section-title">
              <h3>Members</h3>
              <span>{members()?.length ?? 0} ACTIVE</span>
            </div>
            <div class="organization-members">
              <For each={members()} fallback={<p class="organization-empty">Loading membership ledger...</p>}>
                {(member) => (
                  <article>
                    <div class="organization-avatar">{member.name.slice(0, 2).toUpperCase()}</div>
                    <div class="organization-member__identity">
                      <strong>{member.name}</strong>
                      <small>{member.email}</small>
                    </div>
                    <Show
                      when={organization.can("organization.members.manage") && member.role !== "owner"}
                      fallback={<span class="organization-role">{member.role}</span>}
                    >
                      <select
                        class="organization-role-select"
                        disabled={working()}
                        value={member.role}
                        onChange={(event) => void updateRole(member, event.currentTarget.value as EditableRole)}
                      >
                        <option value="admin">admin</option>
                        <option value="member">member</option>
                        <option value="viewer">viewer</option>
                        <option value="billing_admin">billing admin</option>
                      </select>
                    </Show>
                    <div class="organization-member__actions">
                      <Show
                        when={
                          organization.can("organization.owner.transfer") &&
                          member.actorID !== actorID() &&
                          member.role !== "owner"
                        }
                      >
                        <button type="button" disabled={working()} onClick={() => transfer(member)}>
                          Make owner
                        </button>
                      </Show>
                      <Show
                        when={
                          member.role !== "owner" &&
                          (organization.can("organization.members.manage") || member.actorID === actorID())
                        }
                      >
                        <button type="button" disabled={working()} onClick={() => remove(member)}>
                          {member.actorID === actorID() ? "Leave" : "Remove"}
                        </button>
                      </Show>
                    </div>
                  </article>
                )}
              </For>
            </div>
          </section>

          <Show when={organization.can("organization.members.invite")}>
            <section class="organization-drawer__section">
              <div class="organization-drawer__section-title">
                <h3>Invite member</h3>
                <span>7 DAY TOKEN</span>
              </div>
              <form class="organization-invite" onSubmit={invite}>
                <input
                  required
                  type="email"
                  value={inviteEmail()}
                  onInput={(event) => setInviteEmail(event.currentTarget.value)}
                  placeholder="member@company.com"
                />
                <select value={inviteRole()} onChange={(event) => setInviteRole(event.currentTarget.value as EditableRole)}>
                  <option value="member">Member</option>
                  <option value="admin">Admin</option>
                  <option value="viewer">Viewer</option>
                  <option value="billing_admin">Billing admin</option>
                </select>
                <button class="organization-button organization-button--primary" disabled={working()} type="submit">
                  Issue invite
                </button>
              </form>

              <Show when={inviteLink()}>
                {(link) => (
                  <div class="organization-invite-link">
                    <span>{link()}</span>
                    <button type="button" onClick={() => void navigator.clipboard.writeText(link())}>Copy</button>
                  </div>
                )}
              </Show>

              <div class="organization-invitations">
                <For each={invitations()?.filter((item) => item.status === "pending").slice(0, 5)}>
                  {(invitation) => (
                    <div>
                      <span>{invitation.email}</span>
                      <small>{invitation.role} · expires {new Date(invitation.expiresAt).toLocaleDateString()}</small>
                    </div>
                  )}
                </For>
              </div>
            </section>
          </Show>

          <Show when={organization.can("organization.permissions.view")}>
            <OrganizationPermissionCenter />
          </Show>

          <Show when={error()}>
            {(message) => <p class="organization-error">{message()}</p>}
          </Show>

          <footer>
            <div>
              <span>
                {(() => {
                  const state = auth.state()
                  return state.mode === "authenticated" ? state.session.user.email : ""
                })()}
              </span>
              <small>{organization.selected()?.role}</small>
            </div>
            <button type="button" onClick={() => void signOut()}>Sign out</button>
          </footer>
        </aside>
      </Show>
    </>
  )
}
