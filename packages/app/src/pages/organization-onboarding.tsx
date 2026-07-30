import { createEffect, createSignal, Show } from "solid-js"
import { organizationSlugFromName } from "@/context/organization"
import "./organization-control.css"

export function OrganizationOnboarding(props: {
  readonly canCancel: boolean
  readonly error?: string
  readonly onCancel: () => void
  readonly onCreate: (input: { readonly name: string; readonly slug: string }) => Promise<void>
}) {
  const [name, setName] = createSignal("")
  const [slug, setSlug] = createSignal("")
  const [editedSlug, setEditedSlug] = createSignal(false)
  const [submitting, setSubmitting] = createSignal(false)
  const [error, setError] = createSignal<string>()

  createEffect(() => {
    if (editedSlug()) return
    setSlug(organizationSlugFromName(name()))
  })

  const submit = async (event: SubmitEvent) => {
    event.preventDefault()
    setSubmitting(true)
    setError(undefined)
    await props.onCreate({ name: name().trim(), slug: slug().trim() }).catch((cause) => {
      setError(cause instanceof Error ? cause.message : "Organization creation failed")
    })
    setSubmitting(false)
  }

  return (
    <main class="organization-onboarding">
      <div class="organization-onboarding__index">01 / IDENTITY BOUNDARY</div>
      <section>
        <p class="organization-kicker">OpenCode Organization</p>
        <h1>Give your work a boundary.</h1>
        <p class="organization-onboarding__intro">
          Organizations own membership, audit history and the tenant environments that follow. Start with a durable
          name your team will recognize.
        </p>
      </section>

      <form onSubmit={submit}>
        <div class="organization-onboarding__step">
          <span>ORG</span>
          <label>
            Organization name
            <input
              autofocus
              required
              minlength="2"
              maxlength="120"
              value={name()}
              onInput={(event) => setName(event.currentTarget.value)}
              placeholder="Runtime Engineering"
            />
          </label>
        </div>
        <div class="organization-onboarding__step">
          <span>URL</span>
          <label>
            Organization slug
            <div class="organization-slug">
              <small>/org/</small>
              <input
                required
                minlength="3"
                maxlength="63"
                pattern="[a-z0-9][a-z0-9-]{1,61}[a-z0-9]"
                value={slug()}
                onInput={(event) => {
                  setEditedSlug(true)
                  setSlug(event.currentTarget.value.toLowerCase())
                }}
                placeholder="runtime-engineering"
              />
            </div>
          </label>
        </div>

        <Show when={error() ?? props.error}>
          {(message) => <p class="organization-error">{message()}</p>}
        </Show>

        <div class="organization-onboarding__actions">
          <Show when={props.canCancel}>
            <button class="organization-button organization-button--quiet" type="button" onClick={props.onCancel}>
              Cancel
            </button>
          </Show>
          <button class="organization-button organization-button--primary" disabled={submitting()} type="submit">
            {submitting() ? "Creating boundary..." : "Create organization"}
            <span>↗</span>
          </button>
        </div>
      </form>

      <footer>
        <span>OWNER ROLE ASSIGNED AUTOMATICALLY</span>
        <span>MEMBERSHIP AUDIT ENABLED</span>
      </footer>
    </main>
  )
}
