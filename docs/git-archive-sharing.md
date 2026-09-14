# Git archive sharing

Git archive sharing separates the machine that can access Slack from machines that only need a local, searchable copy. The publisher exports compressed JSONL shards and a manifest; subscribers import those snapshots into their own SQLite databases.

Use a private remote. A snapshot contains Slack data and belongs inside the same visibility boundary as the source archive.

Explicit `[sync].include_dms = false` rejects legacy snapshot imports. The
snapshot format stores raw archive tables without evidence that can enforce DM
exclusion. This includes initial subscribe, update, exact/historical restore,
and stale automatic imports. Omitted/true preserves the existing behavior.

To keep using your local archive and API/native sync with DM exclusion, set
`[share].auto_update = false`. Fresh automatic reads and configuration-only
`subscribe --no-import --no-auto-update` remain available. Rejection happens
before saving an importing subscribe configuration or acquiring/importing
snapshot data, media, or successful-import state. Automatic paths still open
the archive to check staleness.

Explicit `[sync].include_dms = false` also blocks `publish`. Legacy table
snapshots cannot enforce DM exclusion across existing rows and derived data.
Keep this archive local. The publish command checks after flag, config and
argument validation, before opening the archive, locking the media cache,
running Git, validating tags or writing snapshots. `--no-commit`, `--no-media`
and tag/push options do not bypass the gate.

With the policy omitted or true, publishing keeps its existing unfiltered
behavior, including archived DMs and drafts. Intake settings do not purge those
rows or certify a snapshot as safe to publish. The publication gate does not
change the optional release notifier that runs before command dispatch.

## Configure the archive

Add a share block to the publisher's config:

```toml
[share]
remote = "git@github.com:your-org/private-slacrawl-archive.git"
repo_path = "~/.slacrawl/share"
branch = "main"
auto_update = true
stale_after = "15m"
```

Keep separate company and personal archives in separate configs, database paths, repositories, and remotes. See [Configuration](configuration.md#visibility-boundaries) for an example split.

## Publish snapshots

The publisher syncs Slack, exports the SQLite archive into the share repository, commits it, and optionally pushes it:

```sh
slacrawl sync --source bot --latest-only
slacrawl publish --remote git@github.com:your-org/private-slacrawl-archive.git --push
```

You can also select an existing local repository, branch, commit message, or immutable snapshot tag:

```sh
slacrawl publish --repo ~/.slacrawl/share --branch main --message "archive: daily refresh" --push
slacrawl publish --tag backup-2026-06-19 --push
```

Relevant flags:

- `--repo` chooses the local git working repository
- `--remote` sets or overrides its remote
- `--branch` chooses the target branch
- `--message` sets the commit message
- `--tag` creates an immutable lightweight tag and requires a commit
- `--no-commit` exports without creating a commit
- `--push` pushes the commit and optional tag to `origin`
- `--no-media` omits cached media from the snapshot

## Subscribe on another machine

`subscribe` writes a reader config, disables live Slack API and desktop sources for that config, clones the share repository, and imports the snapshot:

```sh
slacrawl subscribe --repo ~/.slacrawl/share --db ~/.slacrawl/slacrawl.db git@github.com:your-org/private-slacrawl-archive.git
```

The remote can also be supplied by flag:

```sh
slacrawl subscribe --remote git@github.com:your-org/private-slacrawl-archive.git --stale-after 30m
```

Relevant flags:

- `--repo` chooses the local clone path
- `--db` chooses the reader's SQLite file
- `--branch` chooses the tracked branch
- `--remote` stores the remote without requiring a positional argument
- `--stale-after` controls when read-time refresh considers the snapshot stale
- `--no-auto-update` disables read-time refresh
- `--no-import` skips the initial import
- `--no-media` skips restoring cached media

## Update and restore

`update` pulls and safely merges a newer snapshot:

```sh
slacrawl update
slacrawl update --repo ~/.slacrawl/share --branch main
```

A routine merge preserves destination-only rows, local sync state and embedding work, event history, and newer message, user, or channel tombstones. A row missing from a snapshot is not treated as deleted.

Use `--restore` only when the reader should exactly match a snapshot:

```sh
slacrawl update --restore
slacrawl update --restore --ref backup-2026-06-19
```

`--ref` requires `--restore` and accepts a tag, branch, or commit. Historical restores read Git objects without changing the share checkout's current branch or working tree.

## Automatic refresh

When `auto_update = true`, these read commands import a stale snapshot before querying:

- `report`
- `search`
- `messages`
- `mentions`
- `sql`
- `users`
- `channels`
- `digest` and `analytics`
- `files`

`stale_after` sets the refresh age. `status` and `doctor` only report the share repository, last import time, and freshness state; they do not refresh it.

Non-Desktop `sync`, `tail`, and `files fetch` also refresh stale snapshots before their source operations. These imports obey the same DM policy. Desktop-only `sync --source desktop`/`wiretap` and `watch` do not auto-import Git snapshots.

## Snapshot contents and media

Snapshots contain gzipped JSONL table shards plus `manifest.json`. Eligible cached public-channel media is included by default; `--no-media` omits it when publishing, subscribing, or updating.

Readers rebuild the FTS index locally after import. Publishing a snapshot after a local retention purge does not delete absent rows during a routine reader merge. Use an explicit exact restore when a reader must adopt the publisher's retention set.

## Local publisher and subscriber example

For a local or mounted private bare repository:

```sh
# Publisher
slacrawl publish --remote /path/to/private/slacrawl-archive.git --push

# Subscriber
slacrawl subscribe --repo ~/.slacrawl/share --db ~/.slacrawl/slacrawl.db /path/to/private/slacrawl-archive.git
slacrawl search "incident"
```
