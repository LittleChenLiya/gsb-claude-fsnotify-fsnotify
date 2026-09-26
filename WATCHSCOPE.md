# WatchScope: multi-subscriber scopes on a shared Watcher

`ScopeWatcher` (in `watchscope.go`) multiplexes a single `Watcher` over any
number of independent `Scope` subscribers. Each scope has its own `Events`
and `Errors` channels and its own `Close`; closing one scope never affects
the others.

## Semantics

- **Shared native watches.** Paths are normalized (made absolute with
  `filepath.Abs` and cleaned with `filepath.Clean`) and deduplicated, so two
  scopes watching the same directory share a single native watch. The native
  watch is reference-counted and removed only when the last scope using it
  is closed or removes it.
- **Bounded buffers, no head-of-line blocking.** Every scope has a bounded
  `Events` channel (`ScopeWatcher.NewScope(size)`). A slow scope drops
  events instead of blocking the shared fan-in loop, and is notified with
  `ErrScopeGap` on its own `Errors` channel. Gap notifications are
  coalesced: the `Errors` channel is buffered with size 1 and further gaps
  are dropped while one is pending.
- **Error routing.** Errors from the underlying watcher carry no reliable
  path association, so they are treated as global and broadcast to all
  active scopes. Errors from a scope's own `Add` call are returned to (and
  only to) that scope.
- **Exactly-once close.** A scope's `Events` and `Errors` channels are
  closed exactly once, no matter how many times `Close` is called or whether
  the scope, the `ScopeWatcher`, or the underlying `Watcher` is closed
  first. Closing the underlying `Watcher` closes all scopes.
- **Unchanged Watcher API.** `ScopeWatcher` wraps a `*Watcher`; the original
  API keeps working. While a `ScopeWatcher` is active you must not read from
  `Watcher.Events`/`Watcher.Errors` directly (the fan-in loop owns them),
  but `Add`, `Remove`, `WatchList`, and `Close` behave as before.
  `ScopeWatcher.Close` closes all scopes but deliberately does not close the
  underlying `Watcher`.

## Platform limitations

WatchScope inherits all limitations of the underlying backend (see the
package documentation), plus:

- **Case sensitivity.** Path deduplication is by exact string match after
  `Abs`+`Clean`. On case-insensitive filesystems (Windows, macOS by default)
  two spellings that differ only in case (`C:\Dir` vs `c:\dir`) create two
  native watches instead of one shared watch. This is safe (each scope still
  gets its events) but uses one extra native watch.
- **Event name matching.** Events are routed to a scope when the event path
  equals a watched path or is inside it (separator-aware prefix match). On
  Windows the backend reports names using the casing of the path as it was
  added; because scopes add normalized absolute paths, event names are
  consistent with `Scope.Paths()`.
- **kqueue (macOS/BSD).** The kqueue backend opens a file descriptor per
  watched file, not just per directory; sharing a watch between scopes
  reduces the native watch count but the per-file descriptors of one shared
  directory watch are still opened once (not once per scope).
- **Windows buffer overflows.** `ErrEventOverflow` from
  `ReadDirectoryChangesW` is a watcher-level condition; it is broadcast to
  all scopes. Increase the buffer with `AddWith(..., WithBufferSize(...))`
  on the underlying watcher before wrapping it, if needed.
- **Dropped events are not recoverable from the scope.** After an
  `ErrScopeGap` a scope should resync by rescanning its directories; the
  dropped events are not replayed.

## Demo

A 90-second local demo (two scopes sharing one watch, a slow scope hitting
the gap path, and clean shutdown) lives in `cmd/watchscope-demo`:

    go run ./cmd/watchscope-demo            # runs for 90 seconds
    go run ./cmd/watchscope-demo -duration 15s

The gap scenario is reproduced deterministically in the tests with a
controllable fake backend; see `TestWatchScopeGap` in `watchscope_test.go`.
