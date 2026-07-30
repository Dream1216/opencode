import { describe, expect, test } from "bun:test"
import { organizationIDFromStorage, withOrganizationFetch } from "./server"

describe("organization-aware server transport", () => {
  test("reads the selected organization from UI storage", () => {
    expect(organizationIDFromStorage({ getItem: () => " organization_1 " })).toBe("organization_1")
    expect(organizationIDFromStorage({ getItem: () => null })).toBeUndefined()
  })

  test("injects the latest organization into every request", async () => {
    let selected = "organization_1"
    const headers: string[] = []
    const fetcher = (async (_input: RequestInfo | URL, init?: RequestInit) => {
      headers.push(new Headers(init?.headers).get("x-opencode-organization-id") ?? "")
      return new Response(null, { status: 204 })
    }) as typeof globalThis.fetch
    const request = withOrganizationFetch(fetcher, { getItem: () => selected })

    await request("http://localhost/api/project")
    selected = "organization_2"
    await request("http://localhost/api/session")

    expect(headers).toEqual(["organization_1", "organization_2"])
  })
})
