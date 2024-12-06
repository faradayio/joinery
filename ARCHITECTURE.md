# `joinery` Architecture

Here's a high level overview of how `joinery` works.

## Compiler phases

Compilation procedes in several phases:

1. [Tokenize](./src/tokenizer.rs).
   - Split the source into identifiers, punctuation, literals, etc. All tokens contain the original source code, location information, and surrounding whitespace.
2. [Parse into AST](./src/ast.rs).
   - We use the [`peg` crate](https://docs.rs/peg/). This is a [Parsing Expression Grammar](https://en.wikipedia.org/wiki/Parsing_expression_grammar) (PEG) parser. This is a bit _ad hoc_ as grammars go, but `peg` is a very nice library.
   - We make heavy use of `#[derive]` macros to implement the AST types.
3. [Check types](./src/infer/mod.rs).
   - The internal type system is defined in [`src/types.rs`](./src/types.rs). This is distinct from the simplisitic "source level" type system parsed by [`src/ast.rs`](./src/ast.rs), and better suited to doing inference.
   - Name lookup is handled in [`src/scopes.rs`](./src/scopes.rs). Note that SQL requires several different kinds of scopes.
   - Type checking also needs to know about "memory" types (like Trino's UUID) versus "storage" types (like Trino's VARCHAR when using Hive, which doesn't allow storing UUID). And it needs make sure that all appropriate `LoadExpression` and `StoreExpression` values get inserted.
4. [Apply transforms](./src/transforms/mod.rs).
   - A list of transforms is supplied by each database driver.
   - Transforms use Rust pattern-matching to match parts of the AST, and build new AST nodes using `sql_quote!`. Note that `sql_quote!` outputs _tokens_, so we need to call back into the parser. This is closely patterned after Rust programmatic macros using [`syn`](https://docs.rs/syn/) and [`quote`](https://docs.rs/quote/).
   - After applying a transform, we _may_ need to check types again to support later transforms. This works a bit like an LLVM analysis pass, where specific transforms may indicate that the require types, and the harness ensures that valid types are available.
   - The output of a transform must be structurally valid BigQuery SQL, though after a certain point it may no longer type check.
5. [Emit SQL](./src/ast.rs).
   - This consumes AST nodes and emits them as database-specific strings. We prefer to do as much work as possible using AST transforms, but sometimes we can't represent database-specific features in the AST.
6. [Run](./src/drivers/mod.rs).
   - This is a slightly dodgy layer that knows how to run SQL. Mostly it's intended for running our test suites, not for production use. Some of the Rust database drivers have problems reading complex data types back into Rust.

## Key traits

These traits are implemented by many of the nodes in the AST:

- In [`src/tokenizer.rs`](./src/tokenizer.rs).
  - `Spanned` keeps track of where in the source code a token or AST node was found. This includes a `file_id`.
  - `ToTokens` is used to convert AST nodes back into source code. This is used by `sql_quote!` to build up SQL strings.
- In [`src/ast.rs`](./src/ast.rs).
  - `Emit` is used to convert AST nodes into database-specific SQL strings. This can be derived using `#[derive(Emit)]`, in which case it just calls `EmitDefault`. This is where we override and customize how specific parts of the AST are emitted for certain databases.
    - `EmitDefault` can be automatically derived to emit an AST node by printing every token it contains, recursively. This is optional.
  - `Drive` and `DriveMut` are generic AST walking interfaces provided by [`derive-visitor`](https://docs.rs/derive-visitor/). We use these for lots of custom AST traversals, especially transforms.
  - `Node` is a helper trait that indicates that a given type implements most of the tokenizer and AST traits.
- In [`src/infer/mod.rs`](./src/infer/mod.rs).
  - `InferTypes` handles the main part of type inference.
  - `InferColumnName` is what figures out what `SELECT a, b AS c` create two columns named `a` and `c`.

These traits are implemented by types:

- In [`src/unification.rs`](./src/unification.rs).
  - `Unify` is used to combine two types into a single type. It's what helps us determine that `ARRAY(1, 2.0, NULL)` is in fact an `ARRAY<FLOAT64>`.
  - We should probably pull out more shared traits for types.

These traits are implemented by database drivers:

- In [`src/drivers/mod.rs`](./src/drivers/mod.rs).
  - `Locator` implements a URL-like locator for a database.
  - `Driver` provides the main interface for talking to a database. This also provides things like the list of transforms to apply to the AST. This is "trait safe" and can be referred to as `Box<dyn Driver>`.
  - `DriverImpl` is a helper for `Driver`. This knows about how a database represents types and values, and it isn't "trait safe".

These traits are implemented by transforms:

- In [`src/transforms/mod.rs`](./src/transforms/mod.rs).
  - `Transform` provides an interface for custom AST transforms, generally using `DriveMut` to walk the AST and `sql_quote!` to build new AST nodes. If a transform requires up-to-date type information, it must override `requires_types` to return `true`.

See also [`src/scopes.rs`](./src/scopes.rs). We have more than one kind of scope, but the shared trait interface is still in flux.
