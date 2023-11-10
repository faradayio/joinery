# `joinery` Architecture

Compilation procedes in several phases:

1. [Tokenize](./src/tokenizer.rs).
    - Split the source into identifiers, punctuation, literals, etc. All tokens contain the original source code, location information, and surrounding whitespace.
2. [Parse into AST](./src/ast.rs).
    - We use the [`peg` crate](https://docs.rs/peg/). This is a [Parsing Expression Grammar](https://en.wikipedia.org/wiki/Parsing_expression_grammar) (PEG) parser. This is a bit _ad hoc_ as grammars go, but `peg` is a very nice library.
    - We make heavy use of `#[derive]` macros to implement the AST types.
3. [Check types](./src/infer/mod.rs).
    -  The internal type system is defined in [`src/types.rs`](./src/types.rs). This is distinct from the simplisitic "source level" type system parsed by [`src/ast.rs`](./src/ast.rs), and better suited to doing inference.
    -  Name lookup is handled in [`src/scopes.rs`](./src/scopes.rs). Note that SQL requires several different kinds of scopes.
4. [Apply transforms](./src/transforms/mod.rs).
    - A list of transforms is supplied by each database driver.
    - Transforms use Rust pattern-matching to match parts of the AST, and build new AST nodes using `sql_quote!`. Note that `sql_quote!` outputs _tokens_, so we need to call back into the parser. This is closely patterned after Rust programmatic macros using [`syn`](https://docs.rs/syn/) and [`quote`](https://docs.rs/quote/).
    - After applying a transform, we _may_ need to check types again to support later transforms. This works a bit like an LLVM analysis pass, where specific transforms may indicate that the require types, and the harness ensures that valid types are available.
    - The output of a transform must be structurally valid BigQuery SQL, though after a certain point it may no longer type check.
5. [Emit SQL](./src/ast.rs).
    - This consumes AST nodes and emits them as database-specific strings. We prefer to do as much work as possible using AST transforms, but sometimes we can't represent database-specific features in the AST.
6. [Run](./src/drivers/mod.rs).
   - This is a slightly dodgy layer that knows how to run SQL. Mostly it's intended for running our test suites, not for production use. Some of the Rust database drivers have problems reading complex data types back into Rust.
