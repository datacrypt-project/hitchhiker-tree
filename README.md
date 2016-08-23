# Hitchhiker Tree

Hitchhiker trees are a newly invented (by @dgrnbrg) datastructure, synthesizing fractal trees and functional data structures, to create fast, snapshottable, massively scalable databases.

## What's in this Repository?

The hitchhiker namespaces contain a complete implementation of a persistent, serializable, lazily-loaded hitchhiker tree.
This is a sorted key-value datastructure, like a scalable `sorted-map`.
It can incrementally persist and automatically lazily load itself from any backing store which implements a simple protocol.

Outboard is a sample application for the hitchhiker tree.
It includes an implementation of the IO subsystem backed by Redis, and it manages all of the incremental serialization and flushing.

The hitchhiker tree is designed very similarly to how Datomic's backing trees must work--I would love to see integration with [DataScript](https://github.com/tonsky/datascript) for a fully open source [Datomic](http://www.datomic.com).

## Outboard

Outboard is a simple API for your Clojure applications that enables you to make use of tens of gigabytes of local memory, far beyond what the JVM can manage.
Outboard also allows you to restart your application and reuse all of that in-memory data, which dramatic reduces startup times due to data loading.

Outboard has a simple API, which may be familiar if you've ever used Datomic.
Unlike Datomic, however, Outboard trees can be "forked" like git repositories, not just transacted upon.
Once you've created a tree, you can open a connection to it.
The connection mediates all interactions with the outboard data:
it can accept transactions, provide snapshots for querying, and be cloned.

### API Usage Example

```clojure
(require '[hitchhiker.outboard :as ob])

;; First, we'll create a connection to a new outboard
(def my-outboard (ob/create "first-outboard-tree"))

;; We'll get a snapshot of the outboard's current state, which is empty for now
;; Note that snapshots are only valid for 5 seconds, but making a new snapshot is free
;; It would be easy to write an "extend-life" function for snapshots
(def first-snapshot (ob/snapshot my-outboard))

;; This will insert the pair "hello" "world" only into the snapshot
(-> first-snapshot
    (ob/insert "hello" "world")
    (ob/lookup "hello"))
;;=> "world"

;; Inserts must be done in a transaction to persist
(-> (ob/snapshot my-outboard)
    (ob/lookup "hello"))
;;=> nil

;; We can insert some data into it via a transaction
;; The update! function is atomic, just like swap! for atoms
;; update! will pass its transaction function a snapshot of the outboard
(ob/update! my-outboard (fn [snapshot] (ob/insert snapshot "goodbye" "moon")))

;; Since the insert was transacted, it persists
(-> (ob/snapshot my-outboard)
    (ob/lookup "goodbye"))
;;=> "moon"

;; If you'd like, you can "fork" an outboard. Let's fork our outboard.
;; To fork, you just save a snapshot under a new name
(def forked-outboard (ob/save-as (ob/snapshot my-outboard) "forked-outboard"))

;; Now, we can transact into the snapshot, which will not affect other forks
(ob/update! forked-outboard (fn [snapshot] (ob/insert snapshot "goodbye" "sun")))

;; As we can see:
(-> (ob/snapshot my-outboard)
    (ob/lookup "goodbye"))
;;=> "moon"
(-> (ob/snapshot forked-outboard)
    (ob/lookup "goodbye"))
;;=> "sun"
```

You should check out the docstrings/usage of these functions, too:

- `close` will gracefully shut down an outboard connection
- `open` will reopen an outboard (you can only create outboards which don't exist)
- `destroy` will delete all data related to the closed, named outboard
- `lookup` and `lookup-fwd-iter` provide single and ordered sequence access to snapshots

## Background

Outboard is an off-heap functionally persistent sorted map.
This map allows your applications to retain huge data structures in memory across process restarts.

Outboard is the the first library to make use of hitchhiker trees.
Hitchhiker trees are a functionally persistent, serializable, off-heap fractal B tree.
They can be extended to contain a mechanism to make statistical analytics blazingly fast, and to support column-store facilities.

Details about hitchhiker trees, including related work, can be found in `docs/hitchhiker.adoc`.

## Benchmarking

This library includes a detailed, instrumented benchmarking suite.
It's built to enable comparative benchmarks between different parameters or code changes, so that improvements to the structure can be correctly categorized as such, and bottlenecks can be reproduced and fixed.

To try it, just run

    lein bench

The benchmark tool supports testing with different parameters, such as:

- The tree's branching factor
- Whether to enable fractal tree features, just use the B-tree features, or compare to a vanilla Clojure sorted map
- Reordering of delete operations (to stress certain workloads)
- Whether to use the in-memory or Redis-backed implementation

The benchmarking tool is designed to make it convenient to run several benchmarks;
each benchmark's parameters can be separate by a `--`.
This makes it easy to understand the characteristics of the hitchhiker tree over a variety of settings for a parameter.

You can run a more sophisticated experiment benchmark by doing

    lein bench OUTPUT_DIR options -- options-for-2nd-experiment -- options-for-3rd-experiment

This generates an Excel workbooks called "analysis.xlsx" with benchmark results.
For instance, if you'd like to run experiments to understand the performance difference between various values of B (the branching factor), you can do:

    lein bench perf_diff_experiment -b 10 -- -b 20 -- -b 40 -- -b 80 -- -b 160 -- -b 320 -- -b 640

And it will generate lots of data and the Excel workbook for analysis.

If you'd like to see the options for the benchmarking tool, just run `lein bench`.

## Technical details

See the `doc/` folder for technical details of the hitchhiker tree and Redis garbage collection system.

## Gratitude

Thanks to the early reviewers, Kovas Boguta & Leif Walsh.
Also, thanks to Tom Faulhaber for making the Excel analysis awesome!

## License

Copyright © 2016 David Greenberg

Distributed under the Eclipse Public License version 1.0
