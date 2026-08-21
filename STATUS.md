# Status

## diagnosis
Most of this tree is an architecture scaffold. CI was tuned to stay green with continue-on-error, which hid compile gaps.

## works as design notes
- crate boundaries for workflows, history, timers, compensations
- docs for retries and roadmap

## does not work as production software
- full durable history store
- real worker protocol
- end-to-end workflow execution you can rely on

Treat as a design sketch unless a crate gains real tests and a failing CI on break.
