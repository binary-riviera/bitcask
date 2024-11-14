# Bitcask

Reimplementation of the core bitcask protocol used in [Riak](https://riak.com/)
using the original [whitepaper](https://riak.com/assets/bitcask-intro.pdf) in Python.

Where possible I've tried to use all built in libraries.

## TODO:

- [ ] implement read write modes
- [ ] implement keydir sharing across bitcask instances
- [ ] use CRC codes to actually validate the data written and read
- [ ] add logging
- [ ] add comprehensive e2e integration tests
- [ ] add performance tests
- [ ] add pyproject.tomls
