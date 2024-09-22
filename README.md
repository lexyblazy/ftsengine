### Full Text Search Engine

This is an ongoing improvement on [@akrylysov](https://github.com/akrylysov) [Simple FTS Engine Project](https://github.com/akrylysov/simplefts)


Below are the intended improvements and their status:
- [x] Store the index on disk
- [x] Expose searching via a http server interface
- [x] Sort results by relevance.
- [ ] Extend boolean queries to support OR and NOT (OR is done. There's support for partial matches which can be then sorted by relevance)
- [ ] Pagination
- [ ] Support indexing multiple document fields.


I'm also working on blog post to explain the changes and design decisions.

To run this project

```sh
 chmod +x ./start.sh    # grant permissions to the start script
  ./start.sh
```


### RocksDB Setup

It's very important to use the correct versions. There seems to be a lot of breaking changes and compatibility issues between the versions. So use the below versions

- [RocksDB ](https://github.com/facebook/rocksdb) - `v8.11.3`
- [GoRocksDB](https://github.com/linxGnu/grocksdb)  - `v1.8.14`