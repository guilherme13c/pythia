# Contributing to Pythia

If you want to help out with Pythia, you're in the right place.

I'm trying to keep this project as a playground for learning
async Rust and the Actor model, so whether you've been writing
Rust for years or just read the Book, I'm happy to review your PRs.

## Finding something to do

Check the issue tracker.

- Issues labeled **`good first issue`** are usually isolated
  and don't require you to understand the whole architecture.
- Issues labeled **`help wanted`** are a bit heavier but fully scoped out.

If you find an issue you want to tackle, just drop a comment claiming
it so we don't end up doing duplicate work. If you're stuck or don't
know where a specific actor lives, just ask in the issue thread—I'll
help you out.

## Development setup

1. Fork the repo and clone it locally.
2. Branch off `main` for your work (`git checkout -b feature/whatever-it-is`).
3. Make your changes.

## Before you open a PR

Just a few basic sanity checks before you push:

- **Format your code:** Run `cargo fmt`.
- **Listen to Clippy:** Run `cargo clippy -- -D warnings`. Fix
  whatever it yells at you about.
- **Run the tests:** `cargo test`.
- **(Optional) Check performance:** If you touched the crawler parsing
  logic or the embedding pipeline, run `cargo bench` to make sure
  we didn't accidentally tank the performance.

Once that's done, open a PR against `main`. Just write a brief note
in the description about what you changed and link the issue
number (e.g., `Fixes #3`).

Thanks for taking the time to help build this!
