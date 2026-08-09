# Getting Started

```bash
# from the workspace root
cargo build -p cascade-cli
./target/debug/cascade version
./target/debug/cascade start --workflow-type demo
```

That starts a workflow in the in-memory runtime and prints the IDs.

For a real system you would:

1. Stand up a durable history store (not the memory one).
2. Run the server.
3. Run workers that implement your activities.
4. Call StartWorkflow from your application via the JS/Python clients or the CLI.

None of those pieces are fully wired yet — this is still early skeleton.
