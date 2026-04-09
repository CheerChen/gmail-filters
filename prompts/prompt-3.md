# Task

A Python script `audit_filters.py` that audits Gmail filters by checking when each filter last matched an email and how many emails it has matched in total. Supports a `--cleanup` mode to batch-delete stale filters.

## Auth

- Use Google OAuth2 with `google-auth-oauthlib` and `googleapiclient`
- Scopes: `gmail.readonly` and `gmail.settings.basic`
  - Note: `gmail.settings.basic` is confirmed to work for `filters.list()`. Whether it covers `filters.delete()` has not been verified yet — if delete returns 403, log clearly and suggest the user check scope requirements.
- Token flow:
  - Try loading `token.json`. It may be either an authorized-user token OR a raw OAuth client config (the user may have placed the client config there by mistake).
  - If it's a valid authorized token and not expired, use it.
  - If it's expired and has a refresh token, refresh it.
  - If it's expired but cannot be refreshed, fall back to browser auth.
  - If it's a client config or missing or invalid, look for `credentials.json` as fallback, then run `InstalledAppFlow` browser auth and save the resulting token back to `token.json`.
  - Handle all these cases gracefully with clear error messages.

## API constraints (verified by testing)

- `users.labels.list()` does NOT support `pageToken`. Single call, returns all labels.
- `users.settings.filters.list()` does NOT support `pageToken`. Single call, returns all filters.
- `users.messages.list()` DOES support `pageToken` and MUST be paginated for accurate counts.
- `resultSizeEstimate` from `messages.list()` is unreliable. Tested behavior: with small `maxResults` values it returned `201` for queries whose exact counts were `91`, `327`, and `378`; with larger `maxResults` on the same query it returned the true count again. Never use it for display or logic.

## Core logic

1. Call `users.labels.list()` to build label ID → name mapping (single call, no pagination)
2. Call `users.settings.filters.list()` to get all filters (single call, no pagination)
3. For each filter, extract `criteria` and reconstruct a Gmail search query:
   - `criteria.from` → `from:(value)`
   - `criteria.to` → `to:(value)`
   - `criteria.subject` → `subject:(value)`
   - `criteria.query` → use as-is
   - `criteria.negatedQuery` → `-(value)`
   - Combine with spaces (AND)
4. Skip filters with empty criteria

### Counting (precise)

For each filter's reconstructed query, paginate `users.messages.list()` to get an exact count:
- Use `maxResults=500` per page
- Follow `nextPageToken` until exhausted
- Sum the length of `messages[]` across all pages
- This is significantly slower than the old single-call approach — expected and acceptable

### Last seen date

- From the first page of results (most recent first), take the first message ID
- Call `users.messages.get(id=msg_id, format='metadata', metadataHeaders=['Date'])` to get the date
- Parse the Date header, compute days_ago from today

### Rate limiting

- `time.sleep(0.1)` between paginated list calls
- `time.sleep(0.2)` between delete calls

## Output (default mode)

Sort by days_ago descending (stalest first):

```
Last seen    Days ago    Matches    Labels                    Filter Query
-----------  ----------  ---------  ------------------------  ----------------------------------
(no match)   ∞           0          淘宝                        from:(@notice.mc.alimama.com)
2024-08-07   610         47         1Japan/三井住友カード            from:(@smbc-card.com)
2026-04-08   1           312        bitFlyer                  from:(@bitflyer.com)
```

Summary at end:
- Total filters
- Filters with no match at all
- Filters last seen > threshold days ago

## Cleanup mode

When `--cleanup` is passed:

1. Run the full audit (including precise counts)
2. Collect filters where `days_ago > --days` threshold (including "no match")
3. Print the filtered table
4. Print: "Found N stale filters (last seen > {days} days ago)"
5. Prompt once: "Delete all N filters? [y/N]"
6. If confirmed, delete via `users.settings.filters.delete(id=filter_id)` with progress: "Deleted 1/N: from:(xxx) [LabelName]"
7. If any delete fails (including possible 403 scope issues), log the error with full detail and continue
8. Print summary when done

## CLI arguments (`argparse`)

- `--days` : threshold for "stale" (default 180)
- `--all` : show all filters regardless of threshold (audit mode only)
- `--cleanup` : enable deletion mode (requires confirmation)

## Dependencies

`google-auth-oauthlib`, `google-api-python-client`, `tabulate`, `rich`
