import { expect, test } from "bun:test"
import { Schema } from "effect"
import { ConfigReference } from "../../src/config/reference"

test("ConfigReference.Entry encodes plain Git and local reference objects", () => {
  const git = {
    repository: "github.com/Effect-TS/effect-smol",
    description: "Use for Effect implementation details",
  }
  const local = {
    path: "../local-docs",
    hidden: true,
  }

  expect(Schema.encodeSync(ConfigReference.Entry)(git)).toEqual(git)
  expect(Schema.encodeSync(ConfigReference.Entry)(local)).toEqual(local)
})
