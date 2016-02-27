# tree

A functionally persistent, serializable, off-heap fractal B tree.

Support included for Redis.

## Usage

FIXME

## Design of Redis session store

How to ensure that backing data of trees exists as long as it's reachable?
Need to have a time-based expiry on anonymous keys, with named keys as well
Every new tree's root will be added with its timestamp to the list of anonymous roots
We'll have a timer or something that decides when to delete trees from the anon list
We'll also have a set of named roots, which are pointers to saved trees
We'll build this system in the refcounting API

The top level tree API will have you create a wrapped tree w/ a backend & start up bg expiry threads
every n operations we'll flush the datastructure to the backend (should be made nonblocking)
we'll also support a "save-as" operation on the tree

## Still need to do

Comparative benchmark modes--apples to apples (by params) and everything vs. one trial
This will help guide optimization work

Figure out where the huge spikes in operation time come from (tiered resizing?)

We need a more thorough set of tests for the messaging system to saturate deletes, to ensure we don't have more lurking bugs

Implementations of backwards scan, pred, and succ

Write the WAL, back with in-mem and redis, then add to benchmarks

Write the root-finder

Add values and make a public API

Choose splits and sizes based on serialized results?

benchmark dataset like (map + (repeatedly rand) (iterate #(+ 0.01 %) 0.0))
that's a sliding random window, for some random moving write heavy region
with lots of cold nodes

The API will be:
- Create a new, empty fractal tree (with a name)
- Load a fractal tree from named address
- Do an update to a fractal tree
- Get a snapshot of a fractal tree (have fun iterating or making in-mem changes)
- Store a fractal tree to a new place (clone)
The idea is like cloneable db atoms

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
