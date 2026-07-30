const secretKeys = new Set([
  "apikey",
  "token",
  "authtoken",
  "accesstoken",
  "refreshtoken",
  "clientsecret",
  "secret",
  "secretaccesskey",
  "privatekey",
  "password",
  "authorization",
  "proxyauthorization",
  "cookie",
  "setcookie",
  "xapikey",
])

function normalizedKey(key: string) {
  return key.replaceAll("-", "").replaceAll("_", "").toLowerCase()
}

function redact(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(redact)
  if (typeof value !== "object" || value === null) return value
  return Object.fromEntries(
    Object.entries(value).flatMap(([key, nested]) =>
      secretKeys.has(normalizedKey(key)) ? [] : [[key, redact(nested)]],
    ),
  )
}

export function publicSaasConfig<T>(config: T): T {
  return redact(config) as T
}
