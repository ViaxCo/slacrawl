# Configuration

`slacrawl` is configured with TOML at `~/.slacrawl/config.toml` by default.

Starting with Slacrawl v0.9.0, config-saving commands use owner-only permissions
(`0600`) on POSIX systems, including when saving existing configs. Saving removes
group/other read access; shared-account configurations must plan for owner access.

Config path resolution, runtime directories, status payloads, and token
diagnostic formatting are normalized through `crawlkit`. Slack token scopes,
workspace selection, API/Desktop source behavior, and schema compatibility stay
in `slacrawl`.

The config is designed to work with safe defaults:

- SQLite lives under `~/.slacrawl/`
- Slack Desktop is enabled by default
- the desktop path is auto-detected when left blank
- Slack tokens are resolved from environment variables
- external providers receive only explicitly allowlisted environment additions

## Example

```toml
version = 1
workspace_id = ""
db_path = "~/.slacrawl/slacrawl.db"
cache_dir = "~/.slacrawl/cache"
log_dir = "~/.slacrawl/logs"

[[workspaces]]
id = "T01234567"
default = true
# uses:
# SLACK_T01234567_BOT_TOKEN
# SLACK_T01234567_APP_TOKEN
# SLACK_T01234567_USER_TOKEN

[slack.bot]
enabled = true
token_env = "SLACK_BOT_TOKEN"

[slack.app]
enabled = true
token_env = "SLACK_APP_TOKEN"

[slack.user]
enabled = true
token_env = "SLACK_USER_TOKEN"

[slack.desktop]
enabled = true
path = ""

[slack.mcp]
enabled = true
base_url = "https://chatgpt.com/backend-api/wham/apps"
auth_path = "~/.codex/auth.json"
token_env = "CODEX_APPS_ACCESS_TOKEN"
account_id_env = "CODEX_APPS_ACCOUNT_ID"
protocol_version = "2025-03-26"
connector_id = ""
channel_types = "public_channel,private_channel"
page_size = 100
search_limit = 20
max_pages = 250

[sync]
concurrency = 4
repair_every = "30m"
desktop_refresh_every = "5m"
full_history = true

[search]
default_mode = "auto"

[share]
remote = ""
repo_path = "~/.slacrawl/share"
branch = "main"
auto_update = true
stale_after = "15m"
```

## Archive Schema Upgrades

Starting with Slacrawl v0.9.0, the first writable open of a pre-v8 database upgrades
its SQLite schema version to 8 in a transaction. This is separate from the TOML
configuration version.
Read-only inspection with automatic share updates disabled can inspect a valid
v7 archive without migrating it; `init` only writes configuration. A command such
as `sync` opens the archive writable and can migrate it even if it subsequently
fails for missing credentials.

The one-time upgrade removes `history_coverage_v1` checkpoints for the exact
`api-bot` and `api-user` sources. Pre-v8 checkpoints cannot reliably distinguish
locally completed scans from checkpoints imported from another archive. Messages,
retention floors and seeds, provider/MCP state, and other sync state are retained.
The migration performs no network requests or backfill and does not mark any
history complete. Later writable opens preserve newly earned checkpoints.

The next ordinary API sync treats that coverage as unknown and may repeat history
requests from the existing retention floor. Without a floor, it can scan all
accessible history, with corresponding API cost and rate-limit delays. A newer
saved message does not prove that older history was read. Successful scans earn
local completion checkpoints; interrupted scans retain their pending interval.
`--latest-only` selects previously observed channels; it does not bound the
history interval or request count.

Git-share snapshots retain manifest version 1 and their existing table format,
but these API coverage checkpoints are local-only and are not exported. Imports
consume and validate older snapshots containing them without applying them:
merge preserves the destination's own checkpoints, while explicit restore clears
coverage and leaves it unknown. Data-only snapshots do not back up API completion
state.

Before the first writable open, stop every old process accessing the database.
Complete a consistent pre-upgrade backup using SQLite's
[Online Backup API](https://sqlite.org/backup.html) and verify that the backup
opens independently with the expected schema and data. Retain the old binary and
configuration alongside the database backup. Copying only a live database's main
file can omit WAL data. Old binaries reject schema v8 on a new open, but that
check does not stop already-open clients. Do not mix old and new running processes.

Recovery with an older binary requires restoring the consistent pre-upgrade
database backup and configuration after stopping the upgraded processes. Account
for writes since that backup: restoring it discards those writes unless a
separately reviewed recovery preserves them. Do not lower `user_version` or
attempt an in-place downgrade. A data-only snapshot readable by an older binary
does not make old-version syncing or opening an upgraded database safe.

## Workspace Selection

`workspace_id` remains the default CLI workspace.

Use `[[workspaces]]` when you want separate bot/app/user tokens per Slack workspace, especially for multi-workspace API sync and live tailing:

```toml
[[workspaces]]
id = "T01234567"
default = true

[[workspaces]]
id = "T08976543"
bot_token_env = "SLACK_CLIENT_BOT_TOKEN"
app_token_env = "SLACK_CLIENT_APP_TOKEN"
user_token_env = "SLACK_CLIENT_USER_TOKEN"
```

Behavior:

- each workspace automatically tries `SLACK_<WORKSPACE_ID>_BOT_TOKEN`, `SLACK_<WORKSPACE_ID>_APP_TOKEN`, and `SLACK_<WORKSPACE_ID>_USER_TOKEN`
- top-level `enabled` flags are inherited, so you do not need to repeat `enabled = true` for every workspace
- `bot_token_env`, `app_token_env`, and `user_token_env` are optional overrides when you do not want the default env naming convention
- `sync --source bot` without `--workspace` runs against every configured `[[workspaces]]` entry
- `tail` without `--workspace` starts one live tail per configured `[[workspaces]]` entry
- `search`, `messages`, `mentions`, `users`, and `channels` accept `--workspace` to filter the shared SQLite database
- `users` and `channels` return 100 rows by default and accept a positive `--limit` override
- if `[[workspaces]]` is empty, the legacy top-level `[slack.*]` token config is used

## Visibility Boundaries

One config/database should represent one Slack visibility boundary: the messages visible to one bot/account/profile. Use ingestion sources to decide how that archive is populated:

- `sync --source bot` is an alias for `sync --source api` and uses Slack bot/user tokens
- `sync --source mcp` fetches from a Slack connector exposed by the configured HTTP JSON-RPC MCP gateway
- `sync --source provider:<name>` imports a configured external archive through a trusted local subprocess
- `sync --source wiretap` is an alias for `sync --source desktop` and reads the local Slack Desktop cache
- `sync --source all` runs token-backed sync first, then desktop enrichment; external providers remain explicit
- `[share]` backs up the current DB and safely merges snapshots by default; it is not a second Slack data source
- exact latest or historical replacement requires `update --restore`

Keep company and personal Slack archives in separate configs, DBs, and git remotes:

```toml
# ~/.slacrawl/company.toml
db_path = "~/.slacrawl/company.db"

[share]
remote = "git@github.com:your-org/company-slacrawl-archive.git"
repo_path = "~/.slacrawl/company-share"
```

```toml
# ~/.slacrawl/personal.toml
db_path = "~/.slacrawl/personal.db"

[share]
remote = "git@github.com:your-user/personal-slacrawl-archive.git"
repo_path = "~/.slacrawl/personal-share"
```

## MCP Connector Source

MCP sync is an additional ingestion source; all reads still use the local slacrawl database. It discovers Slack tools with `tools/list`, calls the connector for users, channels, channel history, and threads, then normalizes the results into the same archive schema used by API and desktop sync.

```bash
slacrawl sync --source mcp --workspace T01234567
slacrawl sync --source mcp --workspace T01234567 --channels C01234567 --since 1772574099.659199
```

The workspace ID is required because connector responses do not reliably carry archive ownership. `connector_id` is optional; when empty, tool discovery matches Slack tools by their names and metadata. Set it when multiple Slack connectors are exposed by one gateway.

The default `transport = "http"` uses Codex's connector gateway:

```toml
[slack.mcp]
enabled = true
transport = "http"
base_url = "https://chatgpt.com/backend-api/wham/apps"
auth_path = "~/.codex/auth.json"
```

HTTP authentication order:

1. The configured `token_env` and optional `account_id_env`.
2. `CODEX_APPS_ACCESS_TOKEN` or `CODEX_CONNECTORS_TOKEN`.
3. The configured `auth_path`, using Codex's `tokens.access_token` and optional `tokens.account_id` fields.

Automatic Codex credentials are restricted to the HTTPS `chatgpt.com` origin,
including redirects. A custom HTTP MCP origin requires a dedicated `token_env`
other than the Codex fallback variables. It never falls back to Codex env/file
credentials when that dedicated variable is missing. The default Codex account
ID is not sent to custom origins; configure a dedicated `account_id_env` when
the custom server needs one. Explicit custom-server tokens and stdio remain
supported.

`max_pages` bounds the text connector's users, channels, channel-history, and thread pagination loops and the native reference adapter's users/channels loops; hitting the bound returns an error instead of silently accepting an incomplete page set. Native history/replies tools do not accept pagination arguments. The Codex HTTP connector accepts at most 20 channel or user search results per request. With `include_dms` omitted/true, explicit channel IDs avoid global channel and user enumeration. Normal MCP sync overlaps the latest stored message timestamp per channel by one hour and rechecks persisted thread roots because Slack does not move an old root into channel history when it receives a new reply; `--full` removes the local channel cursor, while `--latest-only` skips channels with no local history. MCP is an explicit source and is not included in `--source all`.

The text connector's channel search response may omit privacy metadata. With `include_dms` omitted/true, those channels remain locally searchable with kind `mcp_channel`, recording unknown classification. Legacy `publish` still includes these archive rows; that kind is not an export privacy filter.

For the archived MCP reference Slack server, use stdio and export the server's required `SLACK_BOT_TOKEN` and `SLACK_TEAM_ID` variables before running slacrawl:

```toml
[slack.mcp]
enabled = true
transport = "stdio"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-slack"]
env_allowlist = ["SLACK_BOT_TOKEN"]
page_size = 100
search_limit = 100
max_pages = 250
```

The subprocess receives a minimal environment plus known Slack/Codex token variables and any names listed in `env_allowlist`; secrets are not passed through TOML fields. The reference server exposes public or explicitly configured channels and does not paginate channel history. Therefore each sync imports only the latest `page_size` messages returned by `slack_get_channel_history`; `--since` filters that fetched window locally and `--full` cannot extend the server's history window. Channel and user listing still paginate normally. The npm package is deprecated and its upstream repository is archived, but this adapter supports its published tool contract.

Tool discovery selects either the Codex Slack connector contract or the reference `slack_list_channels`, `slack_get_channel_history`, `slack_get_thread_replies`, and `slack_get_users` contract.

### Native response coverage

Under every DM policy, native history and replies require `ok=true` before
processing that response. A missing or false value stops the sync; earlier
committed batches remain. The text connector's response contract is unchanged.

Native `has_more=true` or a nonblank `response_metadata.next_cursor` reports
additional pages. The reference tools cannot request those pages. Slacrawl
processes valid fetched messages and later selected conversations, then returns
an incomplete-history error instead of printing completion or advancing the
successful MCP workspace sync record. Use `--source api` for paginated backfill.
The flag survives local timestamp filtering and empty thread results; a later
successful response cannot clear it. Concrete request, identity, or storage
errors take precedence.

`is_limited=true` reports a separate Slack history/message limit. Slack documents
this flag for earlier messages beyond a free workspace's message limit; it does
not detect every access or retention restriction. Slacrawl preserves the same
successful-sync record and asks the operator to review workspace history
availability. API pagination cannot restore messages Slack does not expose.

These checks preserve valid writes, not whole-sync atomicity. Channel metadata
and message-derived incremental cursors may change even when the old successful
workspace sync record remains. No opaque cursor is stored or printed, and the
checks do not establish complete history or resumable backfill. See Slack's
[history](https://docs.slack.dev/reference/methods/conversations.history/) and
[replies](https://docs.slack.dev/reference/methods/conversations.replies/) contracts.

### MCP direct-message policy

`[sync].include_dms = false` applies to both `--source mcp` and `connector`.
The text connector cannot establish native conversation types, so this setting
stops it immediately after tool discovery, before data calls or archive writes.
Use `--source api` or a qualified native reference MCP server for this policy.
Tool names, connector metadata, channel names, ID prefixes, and a private flag
alone do not establish conversation type.

For the native reference adapter, strict exclusion reads the complete available
channel catalog once, requiring `ok=true` on every page. Explicit IDs require an
exact catalog ID match. Existing selectors and exclusions apply before admission;
all observations for a selected duplicate ID are checked together. Selected
catalog identity is checked before DM classification under every policy: a
present foreign context workspace stops the sync even on a DM or duplicate.
That record cannot supply type evidence for the configured workspace. Native
`is_im`/`is_mpim` flags exclude that identity even if other channel flags appear.
Other selected conversations require consistent public/private native type
flags; missing or conflicting evidence stops the sync before archive writes,
including with `--latest-only`. Fixed counts report skipped DMs. If no
conversations are eligible, workspace/user rows and MCP freshness are untouched,
and output says so.

Under every policy, available observations for selected IDs survive name matching
and deduplication, including observations from earlier name requests. An excluded
alias excludes its whole ID before identity checks; unrelated catalog IDs are
not admitted or validated. Omitted/true retain existing payload selection and
per-name requests. Direct-ID-only selection still avoids enumeration
under those policies, so it does not acquire catalog evidence.

Under every policy, selected catalog and retained message context workspace IDs
must agree with the configured workspace. Every returned channel page and thread
parent/reply must match the requested conversation and thread before the affected
writes. Native message identities are checked before local timestamp filtering
or conversion. Top-level history and thread messages require nonblank timestamps;
replies must not reuse the parent timestamp. Nested metadata and catalog latest
timestamps remain optional. Earlier successful history writes remain if a later
thread fails. Missing context is bound to the operator's configured workspace;
it is not authenticated identity proof. External authors and their team IDs are allowed.

Omitted/true retain the existing request patterns and stored payload projections,
subject to the identity checks above. Previously archived DMs are not
purged. The reference server's bounded history window and MCP cursor-based
coverage limits remain unchanged; admission does not certify complete history,
DM-origin history, or a safe export.

Ctrl-C cancels MCP sync and stops its stdio server, including when the server stops reading requests and fills the stdin pipe.

MCP response failures report the operation, HTTP status or JSON-RPC code when
available, and a fixed reason. Ordinary errors omit server response bodies,
tool names and error text, parser snippets, conversation identifiers, and opaque
cursors. Transport errors retain cancellation/deadline detection and the
credential-origin rejection reason without printing returned URLs. These
diagnostics do not establish conversation scope or remove previously archived
data.

## External Archive Providers

Use `[[providers]]` to adapt another local archive to the canonical SQLite
schema without adding a source-specific integration to `slacrawl` itself.
Each provider is an explicit sync source and is not included in `--source all`.

With `[sync].include_dms = false`, provider v1 sync stops before reading its
checkpoint or launching the adapter. The protocol accepts arbitrary channel
kinds and opaque raw payloads, so it cannot establish DM exclusion. Use API sync
or a supported Slack workspace JSON export with this setting. Omitted/true keep
existing provider behavior, request bytes, and checkpoint scopes.

This gate governs provider intake. The CLI may initialize the archive and check
Git-share freshness first; earlier configuration/share errors keep precedence.
It does not purge existing DMs or certify an archive as safe to export.
`slack.desktop.include_drafts` remains specific to Desktop intake.

```toml
[[providers]]
name = "archive"
command = "/usr/local/bin/archive-provider"
args = ["provide", "--format", "jsonl"]
env_allowlist = ["ARCHIVE_DB_PATH"]
source_rank = 5
batch_size = 1000
```

```bash
slacrawl sync --source provider:archive --workspace T01234567
slacrawl sync --source provider:archive --workspace T01234567 --channels C01234567
slacrawl sync --source provider:archive --workspace T01234567 --full
slacrawl sync --source provider:archive --workspace T01234567 --limit 100
```

Configuration rules:

- `name` is normalized to lowercase and must be unique; whitespace, slashes, and colons are rejected
- `command` is required; `~` or a leading `~/` is expanded, then the path must be absolute
- `args` are passed directly to the executable without shell parsing
- `env_allowlist` names additional variables to copy from the parent environment; values do not belong in TOML
- the subprocess otherwise receives only ordinary runtime variables such as `HOME`, `PATH`, temporary-directory, user, shell, and platform variables when present
- `source_rank` must be greater than `2`; lower numeric ranks win, so provider messages cannot replace canonical rank `1` or `2` data; equal ranks may replace
- `batch_size` defaults to `1000`, accepts `1` through `100000`, and is forwarded to the provider as a preferred upstream batch size

### JSONL protocol

`slacrawl` writes exactly one JSON object followed by a newline to provider stdin,
then closes stdin. Example:

```json
{"type":"request","protocol":"slacrawl-provider-v1","workspace_id":"T01234567","channels":["C01234567"],"checkpoint":"opaque-provider-state","batch_size":1000,"limit":100}
```

`limit` is omitted when no positive limit was requested. The other optional
fields may also be omitted when empty. `checkpoint` is an opaque string that
the provider previously emitted; `slacrawl` never parses it.

Provider stdout is JSONL. Records must appear in this order:

1. One `hello` record before any other output.
2. Zero or more data and checkpoint records.
3. Exactly one terminal `done` record, followed by EOF.

An example response looks like:

```jsonl
{"type":"hello","protocol":"slacrawl-provider-v1","provider":"archive-cache"}
{"type":"workspace","id":"T01234567","name":"Example Workspace","raw_json":{}}
{"type":"user","workspace_id":"T01234567","id":"U01234567","name":"example","raw_json":{}}
{"type":"channel","workspace_id":"T01234567","id":"C01234567","name":"general","kind":"public_channel","raw_json":{}}
{"type":"message","workspace_id":"T01234567","channel_id":"C01234567","ts":"1772574099.659199","user_id":"U01234567","thread_ts":"","text":"Example message","raw_json":{}}
{"type":"checkpoint","entity_type":"workspace","entity_id":"T01234567","value":"opaque-next-state"}
{"type":"done","records":4}
```

`done.records` counts `workspace`, `channel`, `user`, and `message` records; it
does not count `hello`, `checkpoint`, or `done`. Optional fields by record type:

- `workspace`: `name`, `domain`, `enterprise_id`, `raw_json`
- `channel`: `name`, `kind`, `topic`, `purpose`, `is_private`, `is_archived`, `is_shared`, `is_general`, `raw_json`
- `user`: `name`, `real_name`, `display_name`, `title`, `is_bot`, `is_deleted`, `raw_json`
- `message`: `user_id`, `thread_ts`, `text`, `raw_json`
- `done`: `skipped_users`, `skipped_channels`, `skipped_bad_message_id`, `skipped_duplicate_identity`, `skipped_missing_channel`, `skipped_missing_timestamp_sort_key`, `rounded_thread_messages`, `rounded_threads`, `unresolved_thread_messages`, and `unresolved_threads`

Unknown JSON fields are ignored.

Required identity fields are the requested workspace `id`; channel and user
`workspace_id` plus `id`; and message `workspace_id`, `channel_id`, and Slack
`ts`. A message channel must already exist in the database or have appeared
earlier in the stream. An unknown nonempty message user ID is preserved as a
sparse workspace-bound profile that a later real user record may enrich. Empty
names fall back to their IDs, empty channel kinds become `provider_channel`,
and missing or `null` `raw_json` becomes `{}`.

`slacrawl` builds repaired, normalized search text from each message, extracts
mentions, updates FTS, and uses `source_rank` reconciliation for canonical
message rows. Metadata emitted by a provider is sparse enrichment and does not
replace richer existing workspace, channel, or user metadata.

### Checkpoints, scopes, and limits

The normal unfiltered incremental run stores one checkpoint under
`provider:<name>` and the workspace ID. Any `--since`, `--full`,
`--latest-only`, channel filter, exclusion, or `--limit` gets a deterministic
scope-specific checkpoint. This prevents a bounded validation import or a
partial backfill from advancing the normal incremental cursor.

A `checkpoint` record flushes pending records and the checkpoint in one SQLite
transaction. Emit it only after all records covered by its value. If the
provider later exits nonzero or omits `done`, the command fails, but already
committed records and checkpoints remain available for the next retry. A
successful unbounded `--full` promotes its last checkpoint to the matching
incremental scope; a limited full run does not.

`--limit` is intended for validation imports. `slacrawl` forwards a positive
message limit and fails if the provider emits more messages. The limit applies
only to `message` records, not catalog records. Providers must also interpret
and honor `since`, `full`, `latest_only`, `channels`, and `exclude_channels`;
`slacrawl` forwards those values but cannot infer source-specific filtering.

Incremental provider imports enforce stored channel retention floors. `--full`,
or an explicit `--since` older than a channel's floor, is treated as a deliberate
restore and may reintroduce purged history.

### Safety and failure behavior

- treat the executable as trusted code inside the archive visibility boundary
- `slacrawl` restricts environment forwarding but does not sandbox the executable; it retains the caller's filesystem and network permissions
- allowlist only variables the provider needs, especially credentials or archive paths
- keep separate configs and databases for archives with different visibility
- the first record must be `hello` with protocol `slacrawl-provider-v1`
- cross-workspace records, missing required identities or channels, protocol mismatches, output after `done`, and record-count mismatches fail closed
- the process must emit `done`, close stdout, and exit zero for the run to succeed
- EOF without `done` flushes pending records before reporting failure; exceeding `--limit` likewise flushes pending records through the limit before failing
- provider stderr is capped and attached to failures for diagnostics
- provider v1 transfers metadata and message text, not file blobs

## Git Archive Sharing

Use `[share]` when you want one machine to publish a private Slack archive snapshot and other machines to query it locally without Slack API credentials.

```toml
[share]
remote = "git@github.com:your-org/private-slacrawl-archive.git"
repo_path = "~/.slacrawl/share"
branch = "main"
auto_update = true
stale_after = "15m"
```

Behavior:

- `publish` exports gzipped JSONL table shards plus `manifest.json` into `repo_path`
- snapshots include eligible cached public-channel media by default; table shards are gzip-compressed and media files retain their raw cache layout
- `subscribe` writes a git-reader config, disables Slack API and desktop sources for that config, clones the repo, and imports the snapshot
- pass `--db` to `subscribe` when you want the reader archive to use a non-default SQLite file
- `update` pulls and merges changed snapshots; an unchanged manifest can still restore missing media and refresh successful-import state
- `search`, `messages`, `mentions`, `sql`, `users`, `channels`, `report`, `digest`, `analytics`, and `files` auto-refresh stale git-backed snapshots before querying when `auto_update = true`
- `stale_after` controls how old the last successful import can be before the next read pulls/imports again
- `status` and `doctor` only observe the configured share repo and last import / manifest freshness; they do not auto-refresh

With `[sync].include_dms = false`, legacy Git snapshot imports stop because
their raw table format cannot enforce DM exclusion. This covers subscribe,
update, exact/historical restore, and stale automatic imports before read,
non-Desktop sync, tail, or file-fetch commands. Subscribe rejects before saving
its importing configuration; explicit updates reject before opening the archive
or acquiring Git data. Automatic paths open the archive to check staleness,
then reject before acquisition, snapshot/media import, or freshness updates.

Set `[share].auto_update = false` to continue querying the local archive or
syncing API/native sources while retaining DM exclusion. Fresh automatic reads,
`subscribe --no-import`, and Desktop-only sync/watch remain available.
Omitted/true keeps the existing merge and restore behavior. This policy neither
purges existing DMs nor filters `publish`; snapshots can still contain them.

## Token Sources

Each Slack token source is controlled independently.

Text normalization notes:

- malformed UTF-8 is repaired before indexing
- compatibility forms are normalized with NFKC
- zero-width and non-printable control noise is stripped from indexed text
- weird spacing is collapsed so FTS and mentions stay queryable even when Slack/Desktop payloads are messy

### Bot token

When configured, the bot token is primary for API sync:

- channel discovery
- users snapshot
- channel history

An invalid configured bot stops sync; it does not fall back to a user token.
Disable it for user-only API sync or desktop-only operation:

```toml
[slack.bot]
enabled = false
token_env = "SLACK_BOT_TOKEN"
```

### App token

Use the app token with a bot token for live Socket Mode tailing. A user token
plus an app token does not satisfy Tail's bot requirement:

```toml
[slack.app]
enabled = true
token_env = "SLACK_APP_TOKEN"
```

If app tailing is not needed, disable it:

```toml
[slack.app]
enabled = false
token_env = "SLACK_APP_TOKEN"
```

### User token

Without a configured bot, API sync authenticates the user as primary for
workspace validation, channel discovery, profiles and history. This applies to
`api`, its `bot` alias, and the API portion of `all`. User-primary history uses
`api-user` rank 1, never joins channels, and reports ordinary history scope
errors. Its history coverage and successful workspace marker remain separate
from `api-bot`; switching tokens does not borrow the bot's completed interval.

With a bot, the user is optional. Slacrawl uses it for historical replies and
optional DM discovery while the bot retains primary ownership.

When both authenticate, bot and user tokens must belong to the same workspace. API sync and
periodic tail repair reject a valid user token from another workspace before
fetching data or updating the archive. Doctor reports that mismatch as
unavailable user auth with partial thread coverage. Missing or invalid user
tokens retain bot-only coverage when a bot is primary. User-primary auth errors
and cancellation stop Sync. CLI archive initialization and automatic share
checks still occur before API authentication.

```toml
[slack.user]
enabled = true
token_env = "SLACK_USER_TOKEN"
```

If you do not want user-token access at all:

```toml
[slack.user]
enabled = false
token_env = "SLACK_USER_TOKEN"
```

### Doctor coverage ownership

Doctor authenticates a configured user even without a bot and fills the
existing user availability/error fields. Bot credentials remain absent and
Tail remains unavailable without them. Failed user auth is reported as an
unavailable capability rather than a successful user session.

In JSON, `slack_api.thread_coverage` describes global credentials; top-level
`thread_coverage` uses the named-workspace aggregate when present. An archived
`api-user/thread_skip` or pending API thread work downgrades either full result
to partial in both fields.
Individual `workspace_api` diagnostics and stored `status.thread_state` remain
unchanged. Recent channel skips combine `api-bot` and `api-user`, newest first
with channel-ID tie ordering and one limit of 20.

### Retained API threads

Ordinary API sync and `--full` without `--since` revisit eligible roots already
in the selected channels, even when fetched history no longer contains their
reply hints. Roots need a positive reply count or a distinct archived child;
an empty or self-referencing `thread_ts` is supported. Explicit `--since`,
`--full --since`, Tail repair and excluded conversations leave this backlog
untouched. Current-page thread processing keeps its existing scope.

Replies work is saved locally before history can overwrite hints and in the
same transaction as newly fetched page hints. Errors, incomplete replies and
intentional scope skips keep it pending for a later sync. When user replies
are unavailable, bot history can still complete with partial thread coverage;
the saved work remains. Switching from bot-primary to user-primary sync keeps
that replies work without borrowing the bot's history checkpoint.

Successful replies retire only the generation that was processed. Retained
requests and writes recheck that generation and the parent's live ownership;
deletion or renewal during a request discards its stale response. Full cleanup
keeps thread-skip records while API replies work in that workspace is still
pending; another workspace's pending work does not block cleanup. Remaining
retained work runs after complete history traversal and before the completed history
horizon is saved. A replies failure can therefore stop later channel or media
work while preserving committed messages and the pending history interval.

Committed parent tombstones, message removal through the store, and local purge
cancel matching work and its API thread-skip record, even when the pending job
is already absent. Ordinary preparation also reconciles skips backed by stored
tombstones in the selected workspace and channel, including after a Git-share
merge and without a remaining reply hint. Live or missing parents are not
deletion evidence; malformed pending work still requires repair. History polling
does not discover hidden deletion events; see Slack's
[message contract](https://docs.slack.dev/reference/events/message/#hidden-subtypes).
Missing reply metadata is not deletion. The local jobs are excluded from Git
share export/import and freshness timestamps; archive source entry counts still
include them. A full restore clears local work along with replaced archive rows.
This API change does not add MCP backlog processing or certify complete Slack
capture; MCP scope handling is a separate change.

### API history completeness

API sync and periodic tail repair follow every nonempty history/replies cursor,
even when a page is short or empty. After writing a valid page and handling its
scheduled threads, a terminal `has_more = true` without a continuation cursor
stops the scan with an error. slacrawl does not support timestamp pagination
to continue without a cursor. Unchanged responses leave the same interval
pending on retry.

History `is_limited = true` is retained across accessible pages. Once those
pages have been traversed, the scan reports that completeness of the requested
interval is uncertified. Slack defines this flag for earlier messages beyond
the free-workspace message limit. It does not establish that a particular
bounded interval is missing messages or detect every access/retention limit;
narrowing `--since` is not a completeness bypass.

These checks apply under every DM policy. Valid fetched rows remain stored,
but the previous coverage `Latest` and `Complete` stay unchanged, the attempted
lower bound remains in `Pending`, and ordinary workspace success does not
advance. A corrected retry resumes that pending interval and can complete it.
Concrete request, validation, write, and cancellation failures keep their
existing errors. One-message capability probes remain independent of scan
completion.

See Slack's [history contract](https://docs.slack.dev/reference/methods/conversations.history/#message-types),
[replies pagination](https://docs.slack.dev/reference/methods/conversations.replies/#pagination),
and [cursor pagination guidance](https://docs.slack.dev/apis/web-api/pagination/).

### API and tail direct-message policy

For API sync, `[sync].include_dms` defaults to whether a user token is configured.
Set it to `false` to exclude IM and MPIM conversations before channel metadata,
history checkpoints, or messages are written.
Explicit `is_im`/`is_mpim` flags take exclusion precedence even when other
channel flags are present.
For other selected conversations, missing or conflicting native channel flags
fail the sync instead of being treated as complete.
API sync channel allow-lists and excluded names still apply before this check.
Periodic tail repair lists public/private channels and shares this explicit
exclusion check, but does not discover DMs or carry API sync channel selectors.

All API policies reject missing conversation IDs, conflicting context workspace
IDs, and mismatched channel IDs in typed latest-message, history, or reply
payloads. A rejected page is not written; earlier pages and unfinished coverage
remain available for retry. Slack Connect authors and conversation hosts may
belong to other workspaces and do not trigger this check.

API history/replies and periodic repair also reject empty or whitespace-only
top-level message timestamps before writing any part of that page. Accepted
timestamps stay unchanged; nested metadata and catalog latest timestamps remain
optional, and native replies may echo their parent. Failure preserves earlier
pages and the previous successful workspace state; retry resumes the pending
history interval.

For live Socket Mode events, omitted/true keeps accepting delivered DMs even
without a user token. Explicit `false` skips `im`, `mpim`, and the retired
workspace-app `app_home` DM event type before content normalization or writes,
then acknowledges the intentional skip. Native `channel` and `group` message
events need no extra request. Missing or unrecognized types, including ordinary
edit/delete envelopes, and rename/archive/unarchive events require a fresh
[`conversations.info`](https://docs.slack.dev/reference/methods/conversations.info/)
lookup with the bot token. Grant the relevant `channels:read`, `groups:read`,
`im:read`, or `mpim:read` access. Lookup failures and identity mismatches stop
tailing without acknowledging that event. After identity validation, explicit
`is_im`/`is_mpim` flags cause an intentional skip and ACK even when other channel
flags are present. Otherwise, unknown/conflicting channel flags stop tailing
without ACK; correct the access/configuration problem before restarting.

These lookups run before ACK and use the existing rate-limit retry behavior.
They can delay ACKs beyond Slack's delivery deadline; no prompt-ACK guarantee
is made for events requiring lookup. Results are not cached, and the returned
conversation and its latest message are not stored. Channel metadata updates
only affect existing rows in the authenticated workspace; missing or foreign
rows are intentional no-ops under every policy. Retained message channel IDs
must agree with the envelope, including nested edited/deleted/root messages.
Slack Connect event/author workspace IDs and differing event/message timestamps
are not conversation identity conflicts.

This controls future API/tail/desktop/MCP and Slack-export intake and rejects
legacy Git share imports and external provider v1 sync. It does not purge
archived DMs. Desktop uses the policy as described below; MCP uses the native
evidence requirements above. It does not certify the archive or a Git share as safe to publish:
admitted messages can contain sensitive text
and file metadata, and a current channel type does not establish that its
history lacks messages from a converted group DM.

## Slack Export Admission

`[sync].include_dms = false` applies to import, including `--dry-run` and `--force`.
Omitted/true preserve DM inclusion. Strict admission supports a declared Slack
workspace JSON export with `channels.json`, `groups.json`, `dms.json` and/or
`mpims.json` reference catalogs. A present empty array is valid; null catalogs are invalid. Unrelated
directories/ZIPs without those catalogs are unsupported. Single-user/TXT and
other layouts need separate qualification.

DM catalogs or positive native `is_im`/`is_mpim` observations exclude every
occurrence of the same conversation ID. Their message bodies are not decoded.
For non-DM catalogs, if all four discriminator keys (`is_channel`, `is_group`,
`is_im`, `is_mpim`) are absent, the catalog supplies public/private type and any
supplied `is_private` must agree. If any discriminator is present, sparse
fallback is disabled: positive native channel flags must establish a public or
private channel. Catalog privacy fills only an absent `is_private`; explicit
native privacy wins. Negative-only, all-false, contradictory, null or malformed
flags cannot establish admission. Recognized native flag names are matched
case-insensitively for classification and the DM veto. Names and ID prefixes
never establish type.

Strict catalogs and admitted message JSON reject repeated decoded object keys
before projection, including nested objects and escaped duplicate keys. Catalog
ID/name and native type/privacy keys also reject case-insensitive duplicates.
Omitted/true keep
the existing JSON decoder behavior.

All catalog IDs and name/ID candidates remain reserved, including excluded and
unused fallback candidates. Directory imports compare device/inode identity on
Linux and macOS; strict directory admission on other platforms requires a ZIP
instead. Contained aliases are allowed only for the same conversation. ZIPs
retain original entries and frozen local-header metadata and reject ambiguous
logical names or cross-conversation payload reuse. Payloads still use the
standard ZIP checksum/decompression path.

Retained raw conversation IDs and `context_team_id` must match their prepared
channel/workspace before projection or timestamp/priority skips. This includes
every case-insensitive occurrence of recognized identity fields and message,
previous_message, root, previous and catalog-only latest objects. External author teams, mentions and file-sharing references are not
enclosing conversation identity. Missing channel identity inherits the catalog.

After body validation, imports check existing workspace ownership for admitted
channel IDs and every retained user ID before workspace, user or channel writes.
Dry-run performs the same checks. Store write-time checks remain in place;
the preflight does not make concurrent imports or the whole import atomic.

The first body scan fixes the name-first/ID-fallback choice and records each
file's identity and digest. Any raw row, even one without a timestamp, selects
the name branch; only zero rows permit ID fallback. The write pass verifies and
decodes the same opened-file buffer. New files/catalog edits wait for a new
preparation; missing, replaced or changed planned files stop execution.
Earlier 500-message commits survive a later failure; the pending remainder
does not commit.

Strict all-excluded imports do not initialize the archive/runtime directories
or write workspace/users. Dry-run opens an existing database read-only, treats
a missing one as empty, and never migrates or repairs it. Read-only errors stay
visible. These promises concern importer archive/runtime initialization, not
the CLI's independent interactive release notice or SQLite WAL sidecar bytes.

Fixed omission counts report excluded conversations. Already archived rows
remain, independent user profiles remain eligible, and import retention/priority
semantics are unchanged. Catalog evidence does not authenticate the producer,
prove complete capture, or establish that a current channel never originated
as a group DM. This is not a public-export minimizer or a safe-to-publish verdict.

## Desktop Source

Desktop ingestion is optional and read-only.

`[sync].include_dms = false` also gates desktop conversation-derived writes.
Omitted/true preserve desktop recovery defaults. Strict exclusion requires
selected public/private channel metadata and omits unknown, conflicting, or
heuristic records; every draft destination must be eligible. Fixed omission
counts explain partial intake. Existing archived rows, independent profiles,
and custom statuses remain. See [Desktop DM exclusion](desktop-mode.md#excluding-direct-messages)
for classification, decode coverage, and retained-history limits.

```toml
[slack.desktop]
enabled = true
path = ""
```

Behavior:

- `enabled = true` turns on desktop sync support
- `path = ""` auto-detects the supported macOS or Linux Slack Desktop path
- `path = "/custom/path"` overrides detection
- `include_drafts` defaults to `true`; set it to `false` to exclude unsent drafts
  from desktop/wiretap sync, `watch`, and the desktop phase of `sync --source all`
  or `hybrid`

```toml
[slack.desktop]
enabled = true
include_drafts = false
```

Excluded drafts do not create messages, channel hints, event history, search
entries, or draft counts in sync output. Desktop snapshots still read the local
cache. This setting does not delete drafts already archived, filter DMs, or
sanitize Git snapshots; use a separate empty database when starting an archive
that must never contain drafts.

To disable desktop ingestion completely:

```toml
[slack.desktop]
enabled = false
path = ""
```

## Sync Settings

### `repair_every`

Used by `tail` to run periodic API reconciliation during live sync.

```toml
[sync]
repair_every = "30m"
```

### `desktop_refresh_every`

Used by `watch` to periodically refresh local Slack Desktop state into SQLite.

```toml
[sync]
desktop_refresh_every = "5m"
```

### `concurrency`

Used by API sync to fan out channel history fetches across workers. Keep the default unless you have a reason to tune it for a specific workspace.

Notes:

- higher values increase API fan-out, not write parallelism inside SQLite
- useful mainly for multi-channel API sync, not single-channel runs
- `--concurrency` on the CLI overrides the config value for that run

### `latest-only`

Use `sync --latest-only` when you want to refresh only channels that already have a stored cursor.

Notes:

- useful for fast publisher jobs that already seeded history once
- channels with no local history are skipped instead of triggering a first-time backfill
- `--full` overrides this behavior and still does the full crawl

## Recommended Profiles

### Desktop only

```toml
[slack.bot]
enabled = false
token_env = "SLACK_BOT_TOKEN"

[slack.app]
enabled = false
token_env = "SLACK_APP_TOKEN"

[slack.user]
enabled = false
token_env = "SLACK_USER_TOKEN"

[slack.desktop]
enabled = true
path = ""
```

### API sync without live tail

```toml
[slack.bot]
enabled = true
token_env = "SLACK_BOT_TOKEN"

[slack.app]
enabled = false
token_env = "SLACK_APP_TOKEN"

[slack.user]
enabled = true
token_env = "SLACK_USER_TOKEN"
```

### API sync with live tail and desktop refresh

```toml
[slack.bot]
enabled = true
token_env = "SLACK_BOT_TOKEN"

[slack.app]
enabled = true
token_env = "SLACK_APP_TOKEN"

[slack.user]
enabled = true
token_env = "SLACK_USER_TOKEN"

[slack.desktop]
enabled = true
path = ""

[sync]
repair_every = "30m"
desktop_refresh_every = "5m"
```
