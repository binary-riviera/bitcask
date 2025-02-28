# Bitcask

Reimplementation of the core bitcask protocol used in [Riak](https://riak.com/)
using the original [whitepaper](https://riak.com/assets/bitcask-intro.pdf) in Python.

Where possible I've tried to use all built in libraries.

## To Do:

- [x] implement read write modes
- [ ] implement keydir sharing across bitcask instances
- [ ] implement sync on put and sync
- [ ] add option to use CRC codes to actually validate the data written and read
- [x] add logging
- [ ] add comprehensive e2e integration tests
- [ ] add performance tests
- [x] add pyproject.toml
- [x] use hint_files when constructing keydir
- [ ] share bitcask db location when instance already exists
- [ ] add mypy type checking as Git hook
- [x] move tests to Pytest
- [x] add Poetry
- [ ] add examples
- [ ] add git hooks for formatting/tests/mypy
- [ ] read file size and port number from environmental variables
- [ ] implement LRU caching for smaller values

## Useful commands:

To run all tests run `poetry run pytest tests`

To generate test coverage run `poetry run coverage run -m pytest tests`

To print test coverage report run `poetry run coverage report -m`

To format using Black run `poetry run black bitcask`

To typecheck using Mypy run `poetry run mypy -p bitcask --check-untyped-defs`
