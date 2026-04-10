# Task

A Python script `merge_domains.py` that merges a source domain into a target domain within the existing Gmail label/filter setup created by `discover_domains.py --apply`.

This is a manual correction tool. When two different registered domains belong to the same brand (e.g. `agoda-messaging.com` and `agoda.com`), their labels and filters were created separately. This script consolidates the source into the target.

## Auth

Use the shared auth from `gmail_common.py`:

- Import `build_gmail_service` from `gmail_common`
- Scopes:
  - `gmail.readonly`
  - `gmail.settings.basic`
  - `gmail.modify`

## API constraints (verified)

- Gmail API has no `filters.update()`. Filters can only be created or deleted.
- `users.labels.list()` returns all labels with `id`, `name`, and `type`.
- `users.settings.filters().list()` returns all filters with `id`, `criteria`, and `action`.
- `users.settings.filters().delete()` deletes a filter by ID.
- `users.settings.filters().create()` creates a new filter.
- `users.messages.batchModify()` can add and remove label IDs in a single call. Max 1000 message IDs per call.
- `users.labels.delete()` deletes a label by ID. The label must have no messages (or Gmail will still delete it, orphaning the label reference on messages).

## Core logic

### Step 0: Resolve source and target

Given `--source <domain>` and `--target <domain>`:

1. List all Gmail filters via `users.settings.filters().list()`
2. List all Gmail labels via `users.labels.list()`
3. Find the source filter: the filter whose `criteria.from` matches `@<source-domain>` (case-insensitive)
4. From the source filter's `action.addLabelIds`, resolve the source label ID, then look up the source label name
5. Find the target filter: the filter whose `criteria.from` matches `@<target-domain>` (case-insensitive)
6. From the target filter's `action.addLabelIds`, resolve the target label ID, then look up the target label name
7. If either source or target cannot be found, exit with a clear error message

### Step 1: Migrate messages

- List all messages with the source label: `users.messages.list(labelIds=[source_label_id])`
- Paginate until all message IDs are collected
- For each batch of up to 1000 messages:
  - `users.messages.batchModify()` with:
    - `addLabelIds: [target_label_id]`
    - `removeLabelIds: [source_label_id]`

### Step 2: Delete source filter

- Delete the source filter by ID via `users.settings.filters().delete()`

### Step 3: Create replacement filter

- Create a new filter with:
  - `criteria.from`: `@<source-domain>`
  - `action.addLabelIds`: `[target_label_id]`
  - `action.removeLabelIds`: `["INBOX"]`
- If a filter with identical `criteria.from` already pointing to the target label exists, skip creation
- This ensures future mail from the source domain is labeled under the target and archived

### Step 4: Delete source label

- Delete the source label by ID via `users.labels.delete()`

## Confirmation flow

Before executing any writes:

1. Print the resolved plan:
   - Source: `<source-domain>` → label `<source-label-name>` (filter ID: `<id>`)
   - Target: `<target-domain>` → label `<target-label-name>` (filter ID: `<id>`)
   - Messages to migrate: `<count>`
   - Actions:
     - Migrate messages: add target label, remove source label
     - Delete source filter
     - Create new filter: `from:@<source-domain>` → target label + archive
     - Delete source label
2. Prompt once: `Proceed with merge? [y/N]`
3. If not confirmed, abort

## Error handling

- If the source domain has no matching filter, exit with error
- If the target domain has no matching filter, exit with error
- If the source and target resolve to the same label, exit with error
- If any step fails during execution, log the error and continue with remaining steps
- The operation should be as idempotent as possible:
  - Re-running after a partial failure should not create duplicates
  - If the replacement filter already exists, skip creation

## CLI arguments (`argparse`)

- `--source` : the domain to merge away (required)
- `--target` : the domain to merge into (required)
- `--label-prefix` : label namespace prefix (default `Domains`), used only for display context

## Output

Print per-step progress:

```text
Source: agoda-messaging.com → label "Domains/Agoda Messaging" (filter: abc123)
Target: agoda.com → label "Domains/Agoda" (filter: def456)
Messages to migrate: 42

Planned actions:
  1. Migrate 42 messages: add "Domains/Agoda", remove "Domains/Agoda Messaging"
  2. Delete filter abc123 (from:@agoda-messaging.com)
  3. Create filter: from:@agoda-messaging.com → "Domains/Agoda" + archive
  4. Delete label: "Domains/Agoda Messaging"

Proceed with merge? [y/N]
```

After execution:

```text
  Messages migrated: 42
  Source filter deleted: abc123
  Replacement filter created: ghi789
  Source label deleted: "Domains/Agoda Messaging"

Merge complete.
```

## Assumption about discover_domains.py output

This script only supports filters created by `discover_domains.py --apply`.

For those filters, the expected shape is exactly:

- `criteria.from = "@<domain>"`
- `action.addLabelIds = [<domain_label_id>]`
- `action.removeLabelIds = ["INBOX"]`

No other filter actions need to be preserved or migrated.
If a resolved source or target filter does not match this shape, exit with a clear error.

## Dependencies

Uses only what is already in `gmail_common.py`: `build_gmail_service`, `execute_request`, `fetch_filters`.
