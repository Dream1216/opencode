export type SqlMigration = {
  readonly id: string
  readonly statements: readonly string[]
}

const baseTenantScopedTables = [
  "project",
  "project_directory",
  "session",
  "message",
  "part",
  "todo",
  "session_message",
  "session_input",
  "session_context_epoch",
  "event_sequence",
  "event",
  "audit_event",
] as const

export const tenantScopedTables = [
  ...baseTenantScopedTables,
  "tenant_member",
  "workspace_tenant_binding",
  "pg_session_projection_shadow",
  "worker_lease",
  "worker_effect",
  "worker_job",
  "team_member",
  "worker_queue_api_nonce",
  "worker_queue_action",
  "worker_queue_action_approval",
  "worker_queue_api_rate_limit",
  "worker_queue_identity_audit",
  "worker_queue_action_approval_event",
  "worker_queue_break_glass",
] as const

const tenantPolicy = (table: string) => [
  `alter table ${table} enable row level security`,
  `alter table ${table} force row level security`,
  `create policy ${table}_tenant_isolation on ${table} using (tenant_id = current_setting('opencode.tenant_id', true)) with check (tenant_id = current_setting('opencode.tenant_id', true))`,
]

export const migrations: readonly SqlMigration[] = [
  {
    id: "p4_4_001_tenant_primitives",
    statements: [
      "create table tenant (id text primary key, name text not null, time_created bigint not null, time_updated bigint not null)",
      "create table tenant_member (tenant_id text not null references tenant(id) on delete cascade, actor_id text not null, role text not null, time_created bigint not null, primary key (tenant_id, actor_id))",
      "create table workspace_tenant_binding (tenant_id text not null references tenant(id) on delete cascade, workspace_id text not null, time_created bigint not null, primary key (tenant_id, workspace_id))",
    ],
  },
  {
    id: "p4_4_002_core_tables",
    statements: [
      "create table project (id text primary key, tenant_id text not null, worktree text not null, vcs text, sandboxes jsonb not null default '[]'::jsonb)",
      "create table project_directory (tenant_id text not null, project_id text not null references project(id) on delete cascade, directory text not null, time_created bigint not null, time_updated bigint not null, primary key (tenant_id, project_id, directory))",
      "create table session (id text primary key, tenant_id text not null, team_id text, actor_id text, project_id text not null references project(id) on delete cascade, workspace_id text, parent_id text, slug text not null, directory text not null, path text, title text not null, version text not null, share_url text, summary_additions integer, summary_deletions integer, summary_files integer, summary_diffs jsonb, metadata jsonb, cost numeric not null default 0, tokens_input bigint not null default 0, tokens_output bigint not null default 0, tokens_reasoning bigint not null default 0, tokens_cache_read bigint not null default 0, tokens_cache_write bigint not null default 0, revert jsonb, permission jsonb, agent text, model jsonb, time_created bigint not null, time_updated bigint not null, time_compacting bigint, time_archived bigint)",
      "create table message (id text primary key, tenant_id text not null, session_id text not null references session(id) on delete cascade, time_created bigint not null, time_updated bigint not null, data jsonb not null)",
      "create table part (id text primary key, tenant_id text not null, message_id text not null references message(id) on delete cascade, session_id text not null, time_created bigint not null, time_updated bigint not null, data jsonb not null)",
      "create table todo (tenant_id text not null, session_id text not null references session(id) on delete cascade, content text not null, status text not null, priority text not null, position integer not null, time_created bigint not null, time_updated bigint not null, primary key (tenant_id, session_id, position))",
      "create table session_message (id text primary key, tenant_id text not null, session_id text not null references session(id) on delete cascade, type text not null, seq bigint not null, time_created bigint not null, time_updated bigint not null, data jsonb not null)",
      "create table session_input (id text primary key, tenant_id text not null, session_id text not null references session(id) on delete cascade, prompt jsonb not null, delivery text not null, admitted_seq bigint not null, promoted_seq bigint, time_created bigint not null)",
      "create table session_context_epoch (tenant_id text not null, session_id text primary key references session(id) on delete cascade, baseline text not null, snapshot jsonb not null, baseline_seq bigint not null)",
    ],
  },
  {
    id: "p4_4_003_event_store",
    statements: [
      "create table event_sequence (tenant_id text not null, aggregate_id text not null, seq bigint not null, owner_id text, primary key (tenant_id, aggregate_id))",
      "create table event (id text primary key, tenant_id text not null, actor_id text, aggregate_id text not null, seq bigint not null, type text not null, data jsonb not null, foreign key (tenant_id, aggregate_id) references event_sequence(tenant_id, aggregate_id) on delete cascade)",
      "create unique index event_aggregate_seq_idx on event (tenant_id, aggregate_id, seq)",
      "create index event_aggregate_type_seq_idx on event (tenant_id, aggregate_id, type, seq)",
      "create unique index session_message_session_seq_idx on session_message (tenant_id, session_id, seq)",
      "create index session_message_session_type_seq_idx on session_message (tenant_id, session_id, type, seq)",
      "create index session_input_session_pending_delivery_seq_idx on session_input (tenant_id, session_id, promoted_seq, delivery, admitted_seq)",
      "create unique index session_input_session_admitted_seq_idx on session_input (tenant_id, session_id, admitted_seq)",
      "create unique index session_input_session_promoted_seq_idx on session_input (tenant_id, session_id, promoted_seq)",
    ],
  },
  {
    id: "p4_4_004_audit",
    statements: [
      "create table audit_event (id text primary key, tenant_id text not null, actor_id text not null, action text not null, resource_type text not null, resource_id text not null, outcome text not null, request_id text, event_id text, metadata jsonb not null default '{}'::jsonb, time_created bigint not null)",
      "create index audit_event_tenant_time_idx on audit_event (tenant_id, time_created)",
      "create index audit_event_tenant_resource_idx on audit_event (tenant_id, resource_type, resource_id, time_created)",
    ],
  },
  {
    id: "p4_4_005_rls",
    statements: baseTenantScopedTables.flatMap(tenantPolicy),
  },
  {
    id: "p4_20_001_session_projection_shadow",
    statements: [
      "create table pg_session_projection_shadow (tenant_id text not null, session_id text not null, aggregate_id text not null, source text not null, last_seq bigint not null, event_count bigint not null, message_count bigint not null, input_admitted_count bigint not null, input_promoted_count bigint not null, projection jsonb not null, projection_hash text not null, time_created bigint not null, time_updated bigint not null, primary key (tenant_id, source, aggregate_id))",
      "create index pg_session_projection_shadow_session_idx on pg_session_projection_shadow (tenant_id, session_id, source)",
      ...tenantPolicy("pg_session_projection_shadow"),
    ],
  },
  {
    id: "p4_28_001_worker_lease_fencing",
    statements: [
      "create table worker_lease (tenant_id text not null, run_id text not null, owner_id text not null, fencing_token bigint not null, status text not null check (status in ('active', 'released', 'completed')), lease_expires_at bigint not null, heartbeat_at bigint not null, time_acquired bigint not null, time_released bigint, time_completed bigint, primary key (tenant_id, run_id))",
      "create index worker_lease_status_expiry_idx on worker_lease (tenant_id, status, lease_expires_at)",
      "create table worker_effect (tenant_id text not null, run_id text not null, effect_key text not null, owner_id text not null, fencing_token bigint not null, payload jsonb not null, time_committed bigint not null, primary key (tenant_id, run_id, effect_key), foreign key (tenant_id, run_id) references worker_lease(tenant_id, run_id) on delete cascade)",
      "create index worker_effect_run_time_idx on worker_effect (tenant_id, run_id, time_committed)",
      ...tenantPolicy("worker_lease"),
      ...tenantPolicy("worker_effect"),
    ],
  },
  {
    id: "p4_32_001_worker_job_queue",
    statements: [
      "create table worker_job (tenant_id text not null, run_id text not null, requested_generation bigint not null, claimed_generation bigint not null default 0, completed_generation bigint not null default 0, reason text not null check (reason in ('wake', 'resume', 'recovery')), status text not null check (status in ('pending', 'running', 'completed', 'failed', 'cancelled')), available_at bigint not null, claim_owner text, claim_token bigint not null default 0, claim_expires_at bigint, attempts integer not null default 0, last_error text, time_created bigint not null, time_updated bigint not null, time_completed bigint, primary key (tenant_id, run_id))",
      "create index worker_job_claim_idx on worker_job (tenant_id, status, available_at, claim_expires_at, time_created)",
      ...tenantPolicy("worker_job"),
    ],
  },
  {
    id: "p4_37_001_worker_queue_admin_governance",
    statements: [
      "create table team_member (tenant_id text not null references tenant(id) on delete cascade, team_id text not null, actor_id text not null, role text not null check (role in ('viewer', 'operator', 'admin', 'owner')), time_created bigint not null, primary key (tenant_id, team_id, actor_id))",
      "create index team_member_actor_idx on team_member (tenant_id, actor_id, team_id)",
      "create table worker_queue_api_nonce (tenant_id text not null, actor_id text not null, nonce text not null, signature_digest text not null, expires_at bigint not null, time_created bigint not null, primary key (tenant_id, actor_id, nonce))",
      "create index worker_queue_api_nonce_expiry_idx on worker_queue_api_nonce (tenant_id, expires_at)",
      "create table worker_queue_action (tenant_id text not null, id text not null, team_id text not null, run_id text not null, action text not null check (action in ('requeue')), expected_generation bigint not null, expected_claim_token bigint not null, requested_by text not null, status text not null check (status in ('pending', 'executed', 'rejected', 'expired')), required_approvals integer not null check (required_approvals >= 2), time_created bigint not null, time_expires bigint not null, time_decided bigint, primary key (tenant_id, id))",
      "create unique index worker_queue_action_pending_idx on worker_queue_action (tenant_id, run_id, action) where status = 'pending'",
      "create index worker_queue_action_status_time_idx on worker_queue_action (tenant_id, status, time_created)",
      "create table worker_queue_action_approval (tenant_id text not null, action_id text not null, actor_id text not null, decision text not null check (decision in ('approve')), time_created bigint not null, primary key (tenant_id, action_id, actor_id), foreign key (tenant_id, action_id) references worker_queue_action(tenant_id, id) on delete cascade)",
      "create index worker_queue_action_approval_action_idx on worker_queue_action_approval (tenant_id, action_id, time_created)",
      ...tenantPolicy("tenant_member"),
      ...tenantPolicy("workspace_tenant_binding"),
      ...tenantPolicy("team_member"),
      ...tenantPolicy("worker_queue_api_nonce"),
      ...tenantPolicy("worker_queue_action"),
      ...tenantPolicy("worker_queue_action_approval"),
    ],
  },
  {
    id: "p4_40_001_worker_queue_operational_governance",
    statements: [
      "create table worker_queue_api_rate_limit (tenant_id text not null, actor_id text not null, scope text not null, window_started_at bigint not null, request_count integer not null, time_updated bigint not null, primary key (tenant_id, actor_id, scope))",
      "create index worker_queue_api_rate_limit_window_idx on worker_queue_api_rate_limit (tenant_id, window_started_at, time_updated)",
      "create table worker_queue_identity_audit (id text primary key, tenant_id text not null, actor_id text, provider text not null, key_id text, method text not null, target text not null, outcome text not null check (outcome in ('allow', 'deny', 'throttle')), reason_code text not null, nonce_digest text not null, credential_digest text not null, time_created bigint not null)",
      "create index worker_queue_identity_audit_actor_time_idx on worker_queue_identity_audit (tenant_id, actor_id, time_created)",
      "create index worker_queue_identity_audit_outcome_time_idx on worker_queue_identity_audit (tenant_id, outcome, time_created)",
      "create table worker_queue_action_approval_event (tenant_id text not null, id text not null, action_id text not null, subject_actor_id text, actor_id text not null, event text not null check (event in ('approve', 'revoke', 'expire')), reason text, identity_provider text not null, time_created bigint not null, primary key (tenant_id, id), foreign key (tenant_id, action_id) references worker_queue_action(tenant_id, id) on delete cascade)",
      "create index worker_queue_action_approval_event_action_time_idx on worker_queue_action_approval_event (tenant_id, action_id, time_created)",
      "create table worker_queue_break_glass (tenant_id text not null, id text not null, incident_id text not null, team_id text not null, run_id text not null, actor_id text not null, identity_provider text not null, reason text not null, expected_generation bigint not null, expected_claim_token bigint not null, credential_digest text not null, status text not null check (status in ('executed')), time_created bigint not null, primary key (tenant_id, id))",
      "create unique index worker_queue_break_glass_incident_idx on worker_queue_break_glass (tenant_id, incident_id)",
      "create index worker_queue_break_glass_run_time_idx on worker_queue_break_glass (tenant_id, run_id, time_created)",
      ...tenantPolicy("worker_queue_api_rate_limit"),
      ...tenantPolicy("worker_queue_identity_audit"),
      ...tenantPolicy("worker_queue_action_approval_event"),
      ...tenantPolicy("worker_queue_break_glass"),
    ],
  },
  {
    id: "p6_0_001_identity_control_plane",
    statements: [
      'create table opencode_identity_user (id text primary key, name text not null, email text not null unique, "emailVerified" boolean not null default false, image text, role text not null default \'user\' check (role in (\'user\', \'platform_admin\')), "createdAt" timestamptz not null default current_timestamp, "updatedAt" timestamptz not null default current_timestamp)',
      'create table opencode_identity_session (id text primary key, "expiresAt" timestamptz not null, token text not null unique, "createdAt" timestamptz not null default current_timestamp, "updatedAt" timestamptz not null default current_timestamp, "ipAddress" text, "userAgent" text, "userId" text not null references opencode_identity_user(id) on delete cascade)',
      'create index opencode_identity_session_user_idx on opencode_identity_session ("userId")',
      'create index opencode_identity_session_expiry_idx on opencode_identity_session ("expiresAt")',
      'create table opencode_identity_account (id text primary key, "accountId" text not null, "providerId" text not null, "userId" text not null references opencode_identity_user(id) on delete cascade, "accessToken" text, "refreshToken" text, "idToken" text, "accessTokenExpiresAt" timestamptz, "refreshTokenExpiresAt" timestamptz, scope text, password text, "createdAt" timestamptz not null default current_timestamp, "updatedAt" timestamptz not null default current_timestamp)',
      'create unique index opencode_identity_account_provider_idx on opencode_identity_account ("providerId", "accountId")',
      'create index opencode_identity_account_user_idx on opencode_identity_account ("userId")',
      'create table opencode_identity_verification (id text primary key, identifier text not null, value text not null, "expiresAt" timestamptz not null, "createdAt" timestamptz not null default current_timestamp, "updatedAt" timestamptz not null default current_timestamp)',
      "create index opencode_identity_verification_identifier_idx on opencode_identity_verification (identifier)",
      "create table opencode_organization (id text primary key, slug text not null unique, name text not null, owner_actor_id text not null references opencode_identity_user(id), status text not null default 'active' check (status in ('active', 'suspended', 'deleted')), time_created bigint not null, time_updated bigint not null)",
      "create table opencode_organization_member (organization_id text not null references opencode_organization(id) on delete cascade, actor_id text not null references opencode_identity_user(id) on delete cascade, role text not null check (role in ('owner', 'admin', 'member', 'viewer', 'billing_admin')), status text not null default 'active' check (status in ('invited', 'active', 'suspended')), time_created bigint not null, time_updated bigint not null, primary key (organization_id, actor_id))",
      "create index opencode_organization_member_actor_idx on opencode_organization_member (actor_id, organization_id)",
      "alter table tenant add column if not exists organization_id text references opencode_organization(id) on delete cascade",
      "alter table tenant add column if not exists slug text",
      "alter table tenant add column if not exists status text not null default 'active'",
      "create unique index tenant_organization_slug_idx on tenant (organization_id, slug) where organization_id is not null and slug is not null",
    ],
  },
  {
    id: "p6_2_001_organization_membership",
    statements: [
      "create table opencode_organization_invitation (id text primary key, organization_id text not null references opencode_organization(id) on delete cascade, email text not null, role text not null check (role in ('admin', 'member', 'viewer', 'billing_admin')), token_digest text not null unique, invited_by text not null references opencode_identity_user(id), accepted_by text references opencode_identity_user(id), status text not null default 'pending' check (status in ('pending', 'accepted', 'rejected', 'revoked', 'expired')), expires_at bigint not null, time_created bigint not null, time_updated bigint not null)",
      "create unique index opencode_organization_invitation_pending_idx on opencode_organization_invitation (organization_id, email) where status = 'pending'",
      "create index opencode_organization_invitation_email_idx on opencode_organization_invitation (email, status, expires_at)",
      "create table opencode_organization_audit (id text primary key, organization_id text not null references opencode_organization(id) on delete cascade, actor_id text not null references opencode_identity_user(id), action text not null, target_type text not null, target_id text not null, outcome text not null check (outcome in ('allowed', 'denied')), metadata jsonb not null default '{}'::jsonb, time_created bigint not null)",
      "create index opencode_organization_audit_org_time_idx on opencode_organization_audit (organization_id, time_created)",
      "create index opencode_organization_audit_actor_time_idx on opencode_organization_audit (actor_id, time_created)",
    ],
  },
  {
    id: "p6_3_001_execution_resource_binding",
    statements: [
      "create table opencode_execution_resource_locator (resource_type text not null check (resource_type in ('project', 'session', 'agent_run')), resource_id text not null, organization_id text not null references opencode_organization(id) on delete cascade, tenant_id text not null references tenant(id) on delete cascade, time_created bigint not null, primary key (resource_type, resource_id))",
      "create index opencode_execution_resource_locator_org_idx on opencode_execution_resource_locator (organization_id, resource_type, resource_id)",
      "create table opencode_execution_resource_binding (resource_type text not null check (resource_type in ('project', 'session', 'agent_run')), resource_id text not null, organization_id text not null references opencode_organization(id) on delete cascade, tenant_id text not null references tenant(id) on delete cascade, actor_id text not null references opencode_identity_user(id), project_id text, session_id text, time_created bigint not null, time_updated bigint not null, primary key (tenant_id, resource_type, resource_id), unique (resource_type, resource_id))",
      "create index opencode_execution_resource_binding_org_idx on opencode_execution_resource_binding (tenant_id, organization_id, resource_type, resource_id)",
      "create index opencode_execution_resource_binding_session_idx on opencode_execution_resource_binding (tenant_id, session_id, resource_type)",
      ...tenantPolicy("opencode_execution_resource_binding"),
    ],
  },
  {
    id: "p6_4_001_permission_center",
    statements: [
      "create table opencode_organization_permission_policy (organization_id text primary key references opencode_organization(id) on delete cascade, version bigint not null default 1 check (version > 0), updated_by text not null references opencode_identity_user(id), time_updated bigint not null)",
      "create table opencode_organization_role_permission (organization_id text not null references opencode_organization(id) on delete cascade, role text not null check (role in ('admin', 'member', 'viewer', 'billing_admin')), permission_key text not null, effect text not null check (effect in ('allow', 'deny')), updated_by text not null references opencode_identity_user(id), time_updated bigint not null, primary key (organization_id, role, permission_key))",
      "create index opencode_organization_role_permission_org_role_idx on opencode_organization_role_permission (organization_id, role, permission_key)",
    ],
  },
  {
    id: "p6_5_001_question_event_recovery_index",
    statements: [
      "create index event_type_aggregate_seq_idx on event (tenant_id, type, aggregate_id, seq)",
    ],
  },
  {
    id: "p7_7_3_001_worker_job_workspace_partition",
    statements: [
      "alter table worker_job add column if not exists workspace_directory text",
      "create index if not exists worker_job_workspace_claim_idx on worker_job (tenant_id, workspace_directory, status, available_at, claim_expires_at, time_created)",
    ],
  },
]

export function allStatements() {
  return migrations.flatMap((migration) => migration.statements)
}

export function validateDraft() {
  const sql = allStatements().join("\n")
  const required = [
    "tenant_id",
    "enable row level security",
    "force row level security",
    "current_setting('opencode.tenant_id', true)",
    "event_aggregate_seq_idx",
    "session_input_session_pending_delivery_seq_idx",
    "audit_event",
    "pg_session_projection_shadow",
    "worker_lease",
    "worker_effect",
    "worker_job",
    "fencing_token",
    "requested_generation",
    "claim_token",
    "workspace_directory",
    "team_member",
    "worker_queue_api_nonce",
    "worker_queue_action",
    "worker_queue_action_approval",
    "worker_queue_api_rate_limit",
    "worker_queue_identity_audit",
    "worker_queue_action_approval_event",
    "worker_queue_break_glass",
    "opencode_identity_user",
    "opencode_identity_session",
    "opencode_identity_account",
    "opencode_identity_verification",
    "opencode_organization",
    "opencode_organization_member",
    "opencode_organization_invitation",
    "opencode_organization_audit",
    "opencode_execution_resource_locator",
    "opencode_execution_resource_binding",
    "opencode_organization_permission_policy",
    "opencode_organization_role_permission",
    "event_type_aggregate_seq_idx",
  ]
  return required.filter((item) => !sql.includes(item))
}
