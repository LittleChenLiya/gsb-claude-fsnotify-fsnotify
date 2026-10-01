# WatchScope: scoped subscriptions on a shared Watcher

The `ScopeManager` (in `watchscope.go`) multiplexes a single `fsnotify.Watcher`
over multiple independent scopes. Each scope subscribes to paths with
`Subscribe` and gets its own `Subscription` with its own `Events` and `Errors`
channels and its own `Close`, without affecting any other scope.

```go
w, _ := fsnotify.NewWatcher()
m := fsnotify.NewScopeManager(w) // takes over w.Events and w.Errors

sub, _ := m.Subscribe("/tmp/dir", 64) // bounded buffer of 64 events
for e := range sub.Events {
    fmt.Println(e)
}
sub.Close() // removes the native watch only if it was the last scope on it
```

## Semantics

- **Shared native watches, reference counted.** Paths are normalized with
  `filepath.Clean` before de-duplication, so `"dir"` and `"dir/"` share one
  native watch. The first scope to subscribe to a path calls `Watcher.Add`;
  the native watch is removed with `Watcher.Remove` only when the last scope
  on that path closes. Closing one scope never affects the others.
- **Bounded buffers, per-scope gaps.** Every subscription has a bounded event
  buffer. A slow subscription drops events and is notified with
  `ErrEventGap` (which wraps `ErrEventOverflow`) on its own `Errors` channel.
  Dropping never blocks the dispatcher, the underlying Watcher, or other
  subscriptions.
- **Error classification.** Errors from the underlying Watcher that mention a
  watched path are delivered only to the scopes watching that path; all other
  errors are broadcast to every scope. Errors reported synchronously by
  `Watcher.Add` are returned to the calling `Subscribe` only.
- **Close exactly once.** `Subscription.Close` and `ScopeManager.Close` are
  idempotent; each subscription's channels are closed exactly once. Closing
  the underlying Watcher closes all subscriptions. `ScopeManager.Close` does
  *not* close the underlying Watcher – the caller keeps owning it, and the
  regular `Watcher` API (`Add`, `Remove`, `Close`, `WatchList`, ...) is
  unchanged.

## Platform limitations

All limitations of the underlying backend apply unchanged; scopes share the
single underlying Watcher and its native resources:

- **Linux (inotify):** every watched path consumes one inotify watch from
  `fs.inotify.max_user_watches`, shared by all scopes on the same Watcher.
  Sharing one native watch per path between scopes reduces pressure on the
  limit compared to one Watcher per consumer.
- **macOS, BSD (kqueue):** every watched file or directory consumes one open
  file descriptor; watching many files can hit the "max open files" limit.
  Scope sharing deduplicates descriptors for identical paths only.
- **Windows (ReadDirectoryChangesW):** the per-watch buffer (default 64K, see
  `WithBufferSize`) is shared by all scopes on a path; a kernel-side overflow
  is reported as `ErrEventOverflow` and broadcast to all scopes, independent
  of the per-subscription `ErrEventGap`.
- **illumos (FEN):** no additional limitations beyond the backend's own.
- **Error path classification is best-effort:** backends do not tag errors
  with a path, so scoping relies on the watched path appearing in the error
  string. Errors that cannot be attributed are broadcast to all scopes.
- Events for a path are matched to scopes by path prefix; watching both a
  directory and one of its subdirectories delivers subdirectory events to
  both scopes, matching the underlying backend's behaviour.

## Demo

`cmd/watchscope-demo` demonstrates shared watches, reference-counted removal,
scoped close, and the gap notification against a real Watcher. It finishes in
a few seconds and is hard-capped at 80 seconds:

```sh
go run ./cmd/watchscope-demo
```

The gap scenario is reproduced deterministically in the tests
(`TestWatchScopeGap` in `watchscope_test.go`) with a controllable fake
backend; run everything including the race detector with:

```sh
go test -race -run TestWatchScope .
```
