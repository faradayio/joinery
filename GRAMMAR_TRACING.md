# Grammar tracing

**WARNING:** This is currently broken.

Run tests with tracing enabled:

```bash
cargo test --features trace
```

```bash
cargo install pegviz --git=https://github.com/fasterthanlime/pegviz.git
pegviz -o trace.html
```

Now take all the test output between these two lines, inclusive:

```txt
[PEG_INPUT_START]
...
[PEG_TRACE_STOP]
```

...and paste it into the standard input of `pegviz`. Then hit control-D. You should then be able to open `trace.html` in your browser and see a nice visualization of the grammar.
