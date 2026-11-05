# Retries

activities fail. the runtime should not pretend otherwise.

each activity carries a `RetryPolicy` in its `ActivityOptions`. defaults:

- max attempts: 5 (0 means no limit)
- initial interval: 1s
- coefficient: 2
- max interval: 100s

delay after attempt n is `initial * coefficient^(n-1)`, capped, then scaled
by a random factor in [0.5, 1) so a batch of failing runs doesn't retry in
lockstep.

an activity error is either retryable (the default, and what `?` gives you)
or fatal (`ActivityError::fatal`). fatal skips the remaining attempts. so
does any message containing one of `non_retryable_errors`.

every attempt is in history: `ActivityStarted`, then `ActivityCompleted` or
`ActivityFailed { will_retry }`. workflow code only sees the last one. a
failure with `will_retry: false` comes back as `Err` from `ctx.activity`.

`start_to_close_timeout` (30s by default) applies per attempt and a timeout
counts as a retryable failure. the other timeout fields are not enforced yet.

after a crash, `recover()` restarts outstanding activities from the next
attempt number. an attempt that was running when the process died will run
again, so activities need to be safe to repeat.

compensations are plain activities scheduled through `Saga`; they get the
same retry policy as anything else.
