export * as ConfigReference from "./reference"

import { Schema } from "effect"

export const Git = Schema.Struct({
  repository: Schema.String,
  branch: Schema.String.pipe(Schema.optional),
  description: Schema.String.pipe(Schema.optional),
  hidden: Schema.Boolean.pipe(Schema.optional),
})
export type Git = typeof Git.Type

export const Local = Schema.Struct({
  path: Schema.String,
  description: Schema.String.pipe(Schema.optional),
  hidden: Schema.Boolean.pipe(Schema.optional),
})
export type Local = typeof Local.Type

export const Entry = Schema.Union([Schema.String, Git, Local])
export type Entry = typeof Entry.Type

export const Info = Schema.Record(Schema.String, Entry)
export type Info = typeof Info.Type
