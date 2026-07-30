import { createSignal, Show } from "solid-js"
import "./saas-auth.css"

type Mode = "sign-in" | "sign-up"

export function SaasAuthPage(props: {
  readonly loading: boolean
  readonly error?: string
  readonly onAuthenticated: () => void
  readonly onRetry: () => void
}) {
  const [mode, setMode] = createSignal<Mode>("sign-in")
  const [name, setName] = createSignal("")
  const [email, setEmail] = createSignal("")
  const [password, setPassword] = createSignal("")
  const [confirmPassword, setConfirmPassword] = createSignal("")
  const [showPassword, setShowPassword] = createSignal(false)
  const [showConfirmPassword, setShowConfirmPassword] = createSignal(false)
  const [submitting, setSubmitting] = createSignal(false)
  const [message, setMessage] = createSignal<string>()

  const changeMode = (next: Mode) => {
    setMode(next)
    setMessage(undefined)
    setConfirmPassword("")
  }

  const submit = async (event: SubmitEvent) => {
    event.preventDefault()
    if (mode() === "sign-up" && password() !== confirmPassword()) {
      setMessage("Passwords do not match.")
      return
    }
    setSubmitting(true)
    setMessage(undefined)
    const response = await fetch(`/api/auth/${mode()}/email`, {
      method: "POST",
      credentials: "include",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify({
        ...(mode() === "sign-up" ? { name: name().trim() } : {}),
        email: email().trim(),
        password: password(),
        rememberMe: true,
      }),
    }).catch(() => undefined)
    setSubmitting(false)
    if (!response) {
      setMessage("Identity service is unreachable.")
      return
    }
    if (!response.ok) {
      const result = (await response.json().catch(() => undefined)) as { message?: string } | undefined
      setMessage(result?.message ?? "The credentials could not be accepted.")
      return
    }
    props.onAuthenticated()
  }

  return (
    <main class="saas-auth">
      <div class="saas-auth__grid" aria-hidden="true" />
      <section class="saas-auth__manifesto">
        <div class="saas-auth__mark">OC</div>
        <p class="saas-auth__eyebrow">OpenCode SaaS / Control Plane</p>
        <h1>
          Build with
          <br />
          boundaries.
        </h1>
        <p class="saas-auth__copy">
          Bring sessions, tools, and model usage into an accountable workspace. Identity, organization policy, and
          audit stay attached to every execution.
        </p>
        <div class="saas-auth__features">
          <div>
            <strong>01</strong>
            <span>Organization-scoped execution</span>
          </div>
          <div>
            <strong>02</strong>
            <span>Versioned role permissions</span>
          </div>
          <div>
            <strong>03</strong>
            <span>Durable activity audit</span>
          </div>
        </div>
        <div class="saas-auth__signal">
          <span />
          IDENTITY CONTROL PLANE ONLINE
        </div>
      </section>

      <section class="saas-auth__panel">
        <div class="saas-auth__panel-head">
          <span>SECURE WORKSPACE</span>
          <span class="saas-auth__status"><i /> READY</span>
        </div>
        <div class="saas-auth__panel-title">
          <h2>{mode() === "sign-in" ? "Welcome back" : "Create your identity"}</h2>
          <p>
            {mode() === "sign-in"
              ? "Sign in to continue to your organization."
              : "Start with a personal identity, then create or join an organization."}
          </p>
        </div>
        <div class="saas-auth__tabs" role="tablist" aria-label="Authentication mode">
          <button classList={{ active: mode() === "sign-in" }} onClick={() => changeMode("sign-in")} type="button">
            Sign in
          </button>
          <button classList={{ active: mode() === "sign-up" }} onClick={() => changeMode("sign-up")} type="button">
            Create account
          </button>
        </div>

        <form onSubmit={submit}>
          <Show when={mode() === "sign-up"}>
            <label class="saas-auth__field">
              <span>Display name</span>
              <div class="saas-auth__input-wrap saas-auth__input-wrap--name">
                <input
                  autocomplete="name"
                  required
                  value={name()}
                  onInput={(event) => setName(event.currentTarget.value)}
                  placeholder="How your team sees you"
                />
              </div>
            </label>
          </Show>
          <label class="saas-auth__field">
            <span>Email address</span>
            <div class="saas-auth__input-wrap saas-auth__input-wrap--email">
              <input
                autofocus
                autocomplete="email"
                inputmode="email"
                required
                type="email"
                value={email()}
                onInput={(event) => setEmail(event.currentTarget.value)}
                placeholder="you@company.com"
              />
            </div>
          </label>
          <label class="saas-auth__field">
            <span>Password</span>
            <div class="saas-auth__input-wrap saas-auth__input-wrap--password">
              <input
                autocomplete={mode() === "sign-in" ? "current-password" : "new-password"}
                minlength="8"
                required
                type={showPassword() ? "text" : "password"}
                value={password()}
                onInput={(event) => setPassword(event.currentTarget.value)}
                placeholder="8 characters minimum"
              />
              <button
                class="saas-auth__visibility"
                type="button"
                tabindex="-1"
                onClick={() => setShowPassword((value) => !value)}
              >
                {showPassword() ? "Hide" : "Show"}
              </button>
            </div>
          </label>
          <Show when={mode() === "sign-up"}>
            <label class="saas-auth__field">
              <span>Confirm password</span>
              <div class="saas-auth__input-wrap saas-auth__input-wrap--password">
                <input
                  autocomplete="new-password"
                  minlength="8"
                  required
                  type={showConfirmPassword() ? "text" : "password"}
                  value={confirmPassword()}
                  onInput={(event) => {
                    setConfirmPassword(event.currentTarget.value)
                    setMessage(undefined)
                  }}
                  placeholder="Repeat your password"
                />
                <button
                  class="saas-auth__visibility"
                  type="button"
                  tabindex="-1"
                  onClick={() => setShowConfirmPassword((value) => !value)}
                >
                  {showConfirmPassword() ? "Hide" : "Show"}
                </button>
              </div>
            </label>
          </Show>

          <Show when={message() ?? props.error}>
            {(value) => (
              <div class="saas-auth__error">
                <p>{value()}</p>
                <Show when={props.error}>
                  <button
                    class="saas-auth__retry"
                    disabled={props.loading}
                    type="button"
                    onClick={() => props.onRetry()}
                  >
                    {props.loading ? "Checking session..." : "Retry session check"}
                  </button>
                </Show>
              </div>
            )}
          </Show>

          <button class="saas-auth__submit" disabled={submitting() || props.loading} type="submit">
            <span>{submitting() ? "Authorizing..." : mode() === "sign-in" ? "Enter workspace" : "Create identity"}</span>
            <span aria-hidden="true">-&gt;</span>
          </button>
        </form>

        <div class="saas-auth__legal">
          <span>HttpOnly session</span>
          <span>Attributed execution</span>
          <span>Auditable access</span>
        </div>
      </section>
    </main>
  )
}
