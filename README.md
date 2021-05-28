# Simple Etcd Tester

The purpose of this little project it to test the compaction/defragmentation
of an etcd cluster to better understand the management of the DB storage size

## TL;DR

### Start

```bash
make kind
make build
make up
make forwards-up # typically in a separate window, but not required
make test-small >out.csv
```

### Done

```bash
make down
```

### About the data

The tester generate a completion status to stderr and the test output
data to stdin (the `out.csv` above). This CSV file can be opened in
your favorite spreadsheet tool and graphs can be generated.
