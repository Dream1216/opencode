export type PlatformRole = "platform_admin" | "user"

export type ActorContext = {
  readonly actorID: string
  readonly email: string
  readonly name: string
  readonly platformRoles: readonly PlatformRole[]
  readonly organizationID?: string
  readonly tenantID?: string
  readonly teamID?: string
}

export type IdentitySession = {
  readonly actor: ActorContext
  readonly sessionID: string
  readonly expiresAt: Date
}

export function platformRole(value: unknown): PlatformRole {
  return value === "platform_admin" ? value : "user"
}
