# SPEC: Live Traffic Dashboard for the Lab 4 Proxy

## 1. Purpose

A live browser page that shows the traffic between the Claude Code harness and the model, as it happens, through `proxy.py`. It exists so a lab participant can read what the harness sends (system prompt, tools, conversation, new message) and what comes back (text, tool calls, results, errors) without reading raw JSON in the terminal.

Scope is a 70-minute lab. Simplicity and a clean UI come before backend features.

## 2. Scope

**In scope**

- Anthropic Messages API format only.
- Live list of requests and replies, newest first.
- Per-request breakdown into: system instructions, tool definitions, conversation so far, new message.
- Tool calls shown beside their results.
- Reply display: text and tool calls.
- Delta view by default, with a toggle for the full request.
- Collapsible long text, readable JSON, visually distinct sections.
- Error highlighting for HTTP 4xx/5xx, `tool_result` with `is_error: true`, and abnormal stream ends.
- Stats per request: input/output tokens, duration. Tool usage counts.
- Filter by tool name.
- In-memory storage only.
- Auth headers always redacted.
- Served on localhost only.

**Out of scope**

- Codex / OpenAI formats.
- Cost or pricing. Tokens and time only.
- Persistence across proxy restarts (no disk log, no database).
- Grouping by session. One flat list.
- Free-text search, filters other than tool name.
- Editing, replaying, or resending requests.
- Authentication for the dashboard page itself (localhost only).
- Any build step, package install, or external CDN.

## 3. Architecture

- **Delivery:** one `dashboard.html` file containing inline CSS and JS. No build step, no frameworks, no CDN. Served by `proxy.py` at `GET /__dashboard`.
- **Live updates:** the page subscribes to a Server-Sent Events endpoint on the proxy (`GET /__dashboard/events`). The proxy pushes one event per request observed and one per reply update.
- **Event model:** the proxy keeps an in-memory list of *exchanges*. Each exchange has an id, a timestamp, the request (parsed JSON body and redacted headers), and a response record that fills in as the stream arrives.
- **Dashboard-internal paths** (`/__dashboard*`) are answered by the proxy and are never forwarded upstream.
- **Forwarding is unchanged.** The dashboard observes traffic; it must not alter headers or bodies, and must not delay the reply stream.
- **Stdlib only** on the Python side.

## 4. Data Model

### Exchange

| Field | Meaning |
|---|---|
| `id` | Sequential integer, assigned at request arrival |
| `started_at` | Time request arrived at proxy |
| `request.method`, `request.path` | As sent by the harness |
| `request.headers` | Redacted copy (see §8) |
| `request.body` | Parsed JSON; raw text kept if it fails to parse |
| `response.status` | HTTP status code, or null until headers arrive |
| `response.events` | Parsed SSE events in order (or the parsed JSON body for non-streamed replies) |
| `response.text` | Concatenated text blocks, built from stream deltas |
| `response.tool_calls` | List of `{id, name, input}` built from `tool_use` blocks and `input_json_delta` |
| `response.usage` | `input_tokens`, `output_tokens`, cache fields if present |
| `response.ended_at` | Time final event or connection close was seen |
| `response.state` | `pending` \| `streaming` \| `complete` \| `aborted` |
| `response.error` | Error description, if any (see §7) |

### Tool result linkage

Tool results are not in the reply. They arrive in a later request as `tool_result` blocks with a `tool_use_id`. The dashboard joins these:

- Each `tool_use` (by `id`) in a reply is matched to the `tool_result` with the same `tool_use_id` in any later request.
- A call with no result yet is shown as **pending**.
- A `tool_result` with no matching call is shown as an orphan, flagged as a warning.

## 5. UI Requirements

### 5.1 Layout

- Single page, one column, newest exchange at the top.
- Each exchange is a card with a header line: `#id`, time, status, duration, input/output tokens, and any error badge.
- Inside each card, in this order (see §5.8):
  1. **Prompt**: the user's actual prompt, in large text, at the very top of the card. Always visible.
  2. **Errors**: the error section, if any. Always visible, directly under the prompt.
  3. **Technical details**: one collapsible `<details>` section, **closed by default**, containing the full breakdown: the **Request** blocks (§5.2), then the **Reply** (§5.3).
- A top bar holds: title, live/paused indicator, **Delta / Full** toggle, tool-name filter, and a clear button (clears the in-memory list in the browser view only).
- Must be usable at 1280px wide and not break below that.

### 5.2 Request breakdown

The request is split into four labelled sections, each with a distinct color and icon:

1. **System** — full system instructions. Shown in full, collapsible. Default collapsed after 20 lines.
2. **Tools** — a list of tool definitions. Each tool shows name and description; its input schema is a collapsed JSON block.
3. **Conversation** — prior messages (role, content blocks). In delta mode, only messages added since the previous request are shown here. In full mode, all prior messages are shown.
4. **New message** — the last element of the `messages` array, always shown in full, never collapsed, visually emphasized.

**Delta definition:** compare the current request's `messages` array with the previous exchange's `messages` array. If the previous array is a prefix of the current one, the delta is the elements after that prefix. If not, show the full request and label it "history changed".

### 5.8 Card layout: prompt first, details collapsed

Goal: a participant reads the card at a glance and only opens the technical view when needed.

- **Prompt block (top of card, always visible).**
  - Shows the text content of the New message (the last element of `messages`): all its `text` blocks, in order, joined with a blank line; or the string content if the message content is a plain string.
  - Shown in large, clear sans-serif text (at least 16px), with line breaks preserved and long lines wrapped. Never collapsed, regardless of length.
  - Labelled "Your prompt". If the New message has no text (for example, it only carries `tool_result` blocks), show a short summary instead, such as `Tool result for toolu_… (error)` or `[image]`, so the block is never empty.
- **Technical details (closed by default).**
  - One `<details>` element with the summary "Technical details", showing the number of sections it holds.
  - Expanding it shows exactly the design in §5.2 and §5.3: System, Tools, Conversation, New message, then Reply (text, tool calls with results, footer).
  - Inside the New message section, the raw message JSON is available as its own collapsed `<details>` block ("raw JSON"), in addition to the labelled rendering.
- **State.** Whether the technical details are open is remembered for each card across live updates, Delta/Full changes and filtering.

### 5.3 Reply breakdown

- **Text** — the model's text, rendered as preformatted text (no Markdown rendering).
- **Tool calls** — one card per `tool_use`: tool name, id, input JSON, and beneath it the matched result (see §4) or a **pending** label.
- **Stop reason** and usage in the reply footer.

### 5.4 Readability rules

- Long text (> 20 lines or > 2,000 characters) is collapsed by default, with a "show more" control and the line count shown.
- JSON is pretty-printed with 2-space indentation, keys and strings in distinct colors, and collapsible objects/arrays.
- Each section type has a fixed color (system, tools, conversation, new message, tool call, tool result, error). Colors must work in light and dark mode and must not be the only cue (each section also has a text label).
- Monospace for all JSON and tool input/output.
- Long lines wrap; no horizontal scroll inside a card.

### 5.5 Live behavior

- New exchanges appear at the top as soon as the request arrives. Their reply fills in as stream events arrive.
- The list does not jump while the user is reading: if the user has scrolled down, new items are added above and a "N new — jump to top" banner is shown instead of auto-scrolling.
- A "paused" state stops rendering new items but keeps receiving them; resuming renders the queued items.
- If the SSE connection drops, the page shows a reconnecting banner and resumes from the last received id.

### 5.6 Filtering

- A dropdown lists every tool name seen so far, plus "All".
- Selecting a tool shows only exchanges where the reply calls that tool or where the request contains a `tool_result` for a call to that tool.

### 5.7 Statistics

Shown in the top bar and per exchange:

- Per exchange: input tokens, output tokens, duration (request arrival to stream end).
- Session totals (since page load or proxy start): exchange count, total input and output tokens, average duration.
- Tool usage: count of calls per tool name.

No cost figures.

## 6. Errors

An exchange is flagged as an error (red badge, red left border, error section directly under the prompt, §5.8) when any of these occur:

1. **HTTP error:** response status is 400 or higher.
2. **Tool error:** a `tool_result` in the request has `is_error: true`. The error is shown on the matching tool call.
3. **Abnormal stream end:** the stream closes without a final `message_stop` event, or the stream contains an `error` event.

Each error shows its type, status or message, and the first 500 characters of the body if available.

## 7. Errors in the Proxy Itself

- If the dashboard code fails, the proxy must still forward traffic. Dashboard recording is wrapped so that exceptions are logged to the terminal and never reach the harness.
- If the request body is not valid JSON, the exchange is still recorded with the raw text and an error flag.

## 8. Security and Privacy

- **Redacted headers** (always, no toggle): `authorization`, `x-api-key`, `cookie`, `set-cookie`, `proxy-authorization`. Shown in the UI as `[redacted]`.
- The dashboard stores only what the page displays. Redaction happens before data is stored in memory, so it is never available to the page.
- Bind to `127.0.0.1` only. Reject requests to `/__dashboard*` whose `Host` header is not `localhost` or `127.0.0.1`, to block DNS rebinding.
- The page sends no data anywhere. No analytics, no external fonts or scripts.

## 9. Acceptance Criteria

1. Starting `proxy.py` and running `ANTHROPIC_BASE_URL=http://localhost:8041 claude` shows each request and reply in the dashboard within 1 second of arrival.
2. Newest exchange is at the top.
3. A request with 3 tools and 10 messages shows four labelled sections; the new message is the last element of the messages array.
4. In delta mode, the second request in a conversation shows only the messages added since the first. Toggling to Full shows all messages.
5. A tool call shows its result in the same card after the next request is sent, and shows "pending" until then.
6. A 500 response, a `tool_result` with `is_error: true`, and a stream cut before `message_stop` each show a visible error highlight.
7. Authorization and x-api-key values never appear in the page, in the SSE stream, or in terminal output.
8. Filtering by tool name hides unrelated exchanges.
9. Long system prompts are collapsed by default and can be expanded.
10. Forwarded requests and replies are byte-identical to what happens without the dashboard.
11. The page works with the proxy stopped mid-session (shows reconnecting, no crash).

## 10. Open Items To Verify Against `proxy.py`

- Confirm how `proxy.py` handles streaming: whether it can tee the response bytes into a parser without changing forwarding.
- Confirm the route for dashboard paths does not collide with any path the harness uses.
- Confirm the proxy's threading or async model supports an open SSE connection alongside forwarding.

## 11. Non-Goals for the Lab Build

Do not add: a database, pricing, session grouping, Markdown rendering, editing or replay, multi-provider parsing, or a framework. If time runs short, cut in this order: filtering, then statistics beyond per-exchange tokens, then collapsible JSON (keep collapsible long text).
