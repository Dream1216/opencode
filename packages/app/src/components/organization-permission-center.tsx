import { createEffect, createMemo, createSignal, For, Show } from "solid-js"
import {
  organizationRequest,
  type OrganizationPermissionKey,
  type OrganizationRole,
  useOrganization,
} from "@/context/organization"

type EditableRole = Exclude<OrganizationRole, "owner">

export function OrganizationPermissionCenter() {
  const organization = useOrganization()
  const [role, setRole] = createSignal<EditableRole>("member")
  const [draft, setDraft] = createSignal<readonly OrganizationPermissionKey[]>([])
  const [saving, setSaving] = createSignal(false)
  const [message, setMessage] = createSignal<string>()
  const policy = organization.permissionPolicy
  const selectedRole = createMemo(() => policy()?.roles.find((item) => item.role === role()))
  const canManage = () => organization.can("organization.permissions.manage")

  createEffect(() => {
    const current = selectedRole()
    setDraft(current?.permissions ?? [])
    setMessage(undefined)
  })

  const groups = createMemo(() => {
    const result = new Map<string, NonNullable<ReturnType<typeof policy>>["catalog"][number][]>()
    for (const item of policy()?.catalog ?? []) {
      const values = result.get(item.group) ?? []
      values.push(item)
      result.set(item.group, values)
    }
    return [...result.entries()]
  })

  const toggle = (permission: OrganizationPermissionKey) => {
    setDraft((current) =>
      current.includes(permission)
        ? current.filter((item) => item !== permission)
        : [...current, permission],
    )
  }

  const save = async () => {
    const current = policy()
    const selected = organization.selected()
    if (current === undefined || selected === undefined || !canManage()) return
    setSaving(true)
    setMessage(undefined)
    await organizationRequest(
      `/api/organizations/${selected.id}/permissions/roles/${role()}`,
      {
        method: "PUT",
        body: JSON.stringify({
          permissions: draft(),
          expectedVersion: current.version,
        }),
      },
    )
      .then(async () => {
        await organization.refreshPermissions()
        setMessage("Policy committed")
      })
      .catch((error) => setMessage(error instanceof Error ? error.message : "Policy update failed"))
    setSaving(false)
  }

  return (
    <section class="organization-drawer__section permission-center">
      <div class="organization-drawer__section-title">
        <h3>Permission center</h3>
        <span>POLICY V{policy()?.version ?? "—"}</span>
      </div>

      <Show
        when={policy()}
        fallback={<p class="organization-empty">Resolving effective policy...</p>}
      >
        {(current) => (
          <>
            <div class="permission-center__identity">
              <span>YOUR EFFECTIVE ACCESS</span>
              <strong>{current().actor.permissions.length}</strong>
              <small>{current().actor.role} role grants</small>
            </div>

            <div class="permission-center__roles" role="tablist" aria-label="Organization roles">
              <For each={current().roles.filter((item) => item.mutable)}>
                {(item) => (
                  <button
                    type="button"
                    classList={{ active: role() === item.role }}
                    onClick={() => setRole(item.role as EditableRole)}
                  >
                    <span>{item.role.replace("_", " ")}</span>
                    <small>{item.permissions.length}</small>
                  </button>
                )}
              </For>
            </div>

            <div class="permission-center__matrix">
              <For each={groups()}>
                {([group, items]) => (
                  <fieldset>
                    <legend>{group}</legend>
                    <For each={items}>
                      {(item) => (
                        <label classList={{ locked: !item.delegable }}>
                          <input
                            type="checkbox"
                            checked={draft().includes(item.key)}
                            disabled={!canManage() || !item.delegable || saving()}
                            onChange={() => toggle(item.key)}
                          />
                          <span>
                            <strong>{item.label}</strong>
                            <small>{item.description}</small>
                          </span>
                          <i>{item.delegable ? item.key : "OWNER ONLY"}</i>
                        </label>
                      )}
                    </For>
                  </fieldset>
                )}
              </For>
            </div>

            <div class="permission-center__commit">
              <span>
                {message() ?? (canManage() ? "Changes use optimistic version control." : "Read-only policy view.")}
              </span>
              <Show when={canManage()}>
                <button
                  type="button"
                  class="organization-button organization-button--primary"
                  disabled={saving()}
                  onClick={() => void save()}
                >
                  {saving() ? "Committing..." : `Commit ${role()} policy`}
                </button>
              </Show>
            </div>
          </>
        )}
      </Show>
    </section>
  )
}
