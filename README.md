# Bitcask

Reimplementation of the core bitcask protocol used in [Riak](https://riak.com/)
using the original [whitepaper](https://riak.com/assets/bitcask-intro.pdf) in Python.

Where possible I've tried to use all built in libraries.

## TODO:

- [ ] implement read write modes
- [ ] implement keydir sharing across bitcask instances
- [ ] implement sync on put and sync
- [ ] use CRC codes to actually validate the data written and read
- [ ] add logging
- [ ] add comprehensive e2e integration tests
- [ ] add performance tests
- [x] add pyproject.toml
- [ ] use hint_files when constructing keydir
- [ ] share bitcask db location when instance already exists
- [ ] add mypy type checking as Git hook
- [ ] move tests to Pytest
- [x] add Poetry

## Useful commands:

To run all tests run `python -m unittest discover tests`.

To format using Black run `black bitcask`

To typecheck using Mypy run `mypy -p bitcask --check-untyped-defs`
