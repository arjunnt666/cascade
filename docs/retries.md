# Retries

activities fail. the runtime should not pretend otherwise.

default policy in the skeleton:
- max attempts: 3
- base delay: 200ms
- multiplier: 2
- max delay: 10s

jitter is not applied yet. when you wire a real clock, add full jitter to avoid thundering herds.

compensations run once per failed saga step unless you mark them reentrant.
