# BubbleTea (Go) TUI — Research Notes

> Pinned for potential future rewrite. Current Textual/Python stack is sufficient.

## Architecture

BubbleTea uses the **Elm Model-View-Update (MVU)** pattern:

```
goroutine (WebSocket) → channel → [serial event loop] → Update() → View() → terminal
```

- **Model** — Go struct holding all app state
- **Update(msg) → (Model, Cmd)** — pure function, processes one message at a time
- **View() → string** — renders full UI as a string; framework diffs it
- **Cmd** — async I/O that runs in a goroutine and returns a message

The serial event loop eliminates the need for locks/mutexes in application code.

## Rendering: v2 Cursed Renderer

BubbleTea v2 uses **ncurses-style cell-level diffing** — `View()` returns a plain string and the renderer only repaints changed terminal cells. This is the key performance difference vs Textual:

| | BubbleTea v2 | Textual |
|---|---|---|
| Diff granularity | Terminal cell | Widget/row |
| Per-frame cost | Near-zero allocations (Go) | Python object overhead |
| Table rebuild | Only changed cells repainted | `clear()` + re-add all rows |
| Remote SSH | Major bandwidth reduction | Standard |

For this app's use case (two tables clearing and rebuilding ~50 rows at 1 Hz), BubbleTea's cell diff would be meaningfully more efficient.

## Streaming Data Pattern

Maps directly onto the current `_data_loop` async pattern:

```go
func waitForData(ch <-chan MarketData) tea.Cmd {
    return func() tea.Msg {
        return <-ch  // blocks goroutine, not the event loop
    }
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
    switch msg := msg.(type) {
    case MarketData:
        m.applyUpdate(msg)
        return m, waitForData(m.dataCh)  // re-queue listener
    }
}
```

External goroutines (e.g. Schwab WebSocket) inject messages via `Program.Send()`.

## Ecosystem

- **Lip Gloss** — CSS-inspired styling (margins, borders, colors, bold/italic)
- **Bubbles** — reusable components: spinner, progress bar, text input, viewport, basic table
- **Evertras/bubble-table** — community DataTable with row styling, frozen columns, filtering, pagination — the closest equivalent to Textual's `DataTable`

Row-level highlight styles (orange/purple/yellow/week52) map to cell-level Lip Gloss styles.

## Trade-offs vs Current Stack

| | BubbleTea | Textual (current) |
|---|---|---|
| Render speed | Faster (Go + cell diffing) | Adequate (Python + widget tree) |
| Table rebuild cost | Lower | Higher (`clear()` + re-add rows) |
| 12 Hz ticker | Goroutine + channel, very cheap | 12 Python callbacks/sec |
| No built-in DataTable | Use `Evertras/bubble-table` | Built-in `DataTable` |
| Debuggability | Printf silent in altscreen | Textual devtools available |
| Rewrite cost | High (full Go port) | Already working |

## When to Revisit

- App is targeted at a slow terminal or remote SSH session
- Table row count grows significantly beyond 50
- Python/Textual CPU usage becomes measurable at 1 Hz
- A Go Schwab client exists or is built (eliminates the Python bridge)
