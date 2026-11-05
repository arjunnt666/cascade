# Changelog

## unreleased

- workflow context does real replay: activities, timers, signals, side effects
- `Cascade` host with retries, cancel, and recovery from file history
- file history store, sequence-checked appends
- http server and `cascade serve`; python/js clients talk to it
- fix: CancelTimer and ContinueAsNew wrote the wrong event payloads
- fix: run status only looked at the last event
- fix: timer cancel did nothing

## 0.1.2

- add STATUS.md (honest scope)
- maintenance commit

## 0.1.1

- retry docs, roadmap stubs

## 0.1.0

- initial public skeleton
