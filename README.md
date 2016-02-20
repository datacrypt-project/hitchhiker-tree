# tree

A functionally persistent, serializable, off-heap fractal B tree.

Support included for Redis.

## Usage

FIXME

## Benchmark analysis

From a cpu perspective, the basic B-tree is 4x slower than a `sorted-set`,
while the fractal B-tree is 5x slower than a `sorted-set`.

In-order insertion into either tree is very efficient; the fractal tree
saves 26% on IO with flushes every 100 writes or 1000 writes.

The fractal tree does even better with random IO--it uses 83% less IO.

The redis backend can reach around 5000 inserts/sec when N=1million

You can generate benchmarking data yourself by running the `tree.bench`
namespace. Just do `lein run -m tree.bench opts...`.

## License

Copyright © 2016 David Greenberg

Distributed under the Eclipse Public License version 1.0
