# Task

A Python script `discover_domains.py` that scans recent inbox emails, extracts sender domains, normalizes them to registered/root domains, and outputs a ranked list of recent high-frequency domains worth reviewing for possible Gmail filters.

This version does NOT try to infer whether a domain is already "covered" by existing filters.

It supports an `--apply` mode that turns the current result set into Gmail automation in one confirmed batch:

- create or reuse labels
- create or reuse filters for future matching mail
- apply those labels to already-existing messages from the scanned result set

## Auth

Use the same auth behavior as the current `audit_filters.py` implementation:

- Use Google OAuth2 with `google-auth-oauthlib` and `googleapiclient`
- Scopes:
  - `gmail.readonly`
  - `gmail.settings.basic`
  - `gmail.modify`
- Token flow:
  - Try loading `token.json`. It may be either an authorized-user token OR a raw OAuth client config.
  - If it is a valid authorized token and not expired, use it.
  - If it is expired and has a refresh token, refresh it.
  - If it is expired but cannot be refreshed, fall back to browser auth.
  - If it is a client config or missing or invalid, look for `credentials.json` as fallback, run `InstalledAppFlow`, and save the resulting token back to `token.json`.
  - Handle all cases gracefully with clear error messages.

## API constraints (verified)

- `users.messages.list()` does support `pageToken` and should be paginated.
- `users.messages.list()` does NOT return sender headers like `From`; it only returns message identifiers. To get sender/domain info, each message must be fetched separately via `users.messages.get(...)`.
- `resultSizeEstimate` is unreliable. Never use it for display or logic.
- `users.labels.create()` requires label/modify-level write permission.
- `users.messages.batchModify()` requires `gmail.modify` and is the mechanism used to apply labels to existing messages.

## Core logic

### Step 1: List recent inbox message IDs

- Query: `in:inbox newer_than:{days}d`
- Paginate with `maxResults=500`, following `nextPageToken`
- Add a protection cap so the script cannot pull an unbounded number of messages
- Default protection cap: `--limit 2000`
- Stop pagination once the cap is reached
- If the cap is hit, make it explicit in the output summary that results were truncated by the safety limit

### Step 2: Fetch sender metadata for each message

- For each message ID, call:
  - `users.messages.get(id=msg_id, format='metadata', metadataHeaders=['From', 'Date'])`
- Extract the sender email address from the `From` header
- Parse out the sender domain from the email address
- Parse the `Date` header to track recency
- Keep a small delay between detail calls:
  - `time.sleep(0.05)` between `messages.get` calls

Important:

- This is expected to be API-expensive
- For 100 emails, expect roughly 101 API calls:
  - 1 `messages.list`
  - 100 `messages.get`
- If more than one `list` page is needed, total requests increase accordingly

### Step 3: Aggregate by domain

- Extract the sender domain from the email address
- Normalize it to the registered/root domain using `tldextract`
- Use a public suffix aware approach so that domains like `.co.jp`, `.com.cn`, and `.co.uk` are handled correctly
- Group by the normalized registered/root domain
- For each domain, collect:
  - Total count of emails
  - Most recent email date
  - Up to 3 distinct sender local parts (before `@`) for display

Examples:

- `noreply@shipment.amazon.co.jp` -> domain `amazon.co.jp`, sample sender `noreply`
- `alert@mail.github.com` -> domain `github.com`, sample sender `alert`
- `news@updates.spotify.com` -> domain `spotify.com`, sample sender `news`

### Step 4: Filter and output

- Exclude domains below the `--min` threshold
- Sort by count descending
- Secondary sort: most recent date descending

## Apply mode

When `--apply` is passed:

1. Run the full discovery flow first
2. Use the currently displayed result set as the apply target set
3. Print the candidate table
4. Print the planned action for each domain:
   - proposed label name
   - proposed filter query
   - existing message count that will be labeled
5. Prompt once:
   - `Apply labels and filters for all N domains? [y/N]`
6. If confirmed, for each domain:
   - create or reuse the label
   - create or reuse the Gmail filter
   - apply the label to already-existing scanned messages for that domain using `users.messages.batchModify()`
7. Print per-domain progress and final summary

### Apply mode rules

- `--apply` means one combined action:
  - create label
  - create filter
  - apply label to existing scanned messages
- Do NOT split this into separate flags
- The operation must be idempotent:
  - if a label already exists, reuse it
  - if an equivalent filter already exists, skip filter creation
  - applying an already-present label to messages is acceptable
- Default label naming:
  - `Domains/<registered-domain>`
- Default filter behavior:
  - match future mail from that domain
  - add the domain label
  - do not archive, delete, or mark as read by default
- If any one domain fails, log the error and continue with the rest
- Never perform writes without explicit y/N confirmation

## Output

```text
Count | Last seen  | Domain       | Sample senders
------+------------+--------------+------------------------
47    | 2026-04-08 | github.com   | noreply, support, reply
12    | 2026-04-01 | amazon.co.jp | orders, shipment
```

Summary at end:

- Total message IDs collected
- Total messages inspected
- Unique domains found
- Domains above threshold (displayed)
- Whether the `--limit` protection cap was hit

When `--apply` is used, also print:

- Labels created
- Labels reused
- Filters created
- Filters reused/skipped
- Domains successfully applied
- Domains failed

## CLI arguments (`argparse`)

- `--days` : time window to scan (default 90)
- `--min` : minimum email count to be listed (default 10)
- `--limit` : maximum number of recent messages to inspect as a safety cap (default 2000)
- `--apply` : create/reuse labels and filters, then apply labels to already-existing scanned messages after a single y/N confirmation
- `--label-prefix` : label namespace prefix (default `Domains`)

## Dependencies

`google-auth-oauthlib`, `google-api-python-client`, `tabulate`, `rich`, `tldextract`
