# Hitchhiker Tree

Hitchhiker trees are a newly invented datastructure, synthesizing fractal trees and functional data structures.

## Outboard

Outboard is a simple API for your Clojure applications that enables you to make tens of gigabytes of local memory, far beyond what the JVM can manage.
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
(def first-snapshot (ob/snapshot my-outboard))

;; This will only insert the pair "hello" "world" into the snapshot
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
;; update! will pass its transction function a snapshot of the tree
(ob/update! my-outboard (fn [snapshot] (ob/insert snapshot "goodbye" "moon")))

;; Since the insert was transacted, it persists
(-> (ob/snapshot my-outboard)
    (ob/lookup "goodbye"))
;;=> "moon"

;; If you'd like, you can "fork" an outboard. Let's fork our outboard.
;; To fork, you must save a snapshot under a new name
(def forked-outboard (ob/save-as (ob/snapshot my-outboard) "forked-outboard"))

;; Now, we can transact into the snapshot, which will not affect other forks
(ob/update! forked-outboard (fn [snapshot] (ob/delete snapshot "goodbye")))

;; As we can see:
(-> (ob/snapshot my-outboard)
    (ob/lookup "goodbye"))
;;=> "moon"
(-> (ob/snapshot forked-outboard)
    (ob/lookup "goodbye"))
;;=> nil
```

See also:

- `close` will gracefully shut down an outboard
- `open` will reopen an outboard (you can only create outboards which don't exist)
- `destroy` will delete all data related to the outboard
- `lookup` and `lookup-fwd-iter` provide single and ordered sequence access to snapshots

## Background

They are a functionally persistent, serializable, off-heap fractal B tree.
They can be extended to contain a mechanism to make statistical analytics blazingly fast, and to support column-store facilities.

The first application for these data structures is as an off-heap functionally persistent sorted map.
This map allows your applications to retain huge data structures in memory across process restarts.

## Benchmarking

This library includes a detailed, instrumented benchmarking suite.
It's built to enable comparative benchmarks between different parameters or code changes, so that improvements to the structure can be correctly categorized as such, and bottlenecks can be reproduced and fixed.

The benchmark tool supports testing with different parameters, such as:

- The tree's branching factor
- Whether to enable fractal tree features, just use the B-tree features, or compare to a vanilla Clojure sorted map
- Reordering of delete operations (to stress certain workloads)
- Whether to use the in-memory or Redis-backed implementation

The benchmarking tool is designed to make it convenient to run several benchmarks;
each benchmark's parameters can be separate by a `--`.
This makes it easy to understand the characteristics of the hitchhiker tree over a variety of settings for a parameter.

You can run a benchmark by doing

    lein bench OUTPUT_DIR options -- options-for-2nd-experiment -- options-for-3rd-experiment

This generates excel workbooks with benchmark results.
For instance, if you'd like to run experiments to understand the performance difference between various values of B (the branching factor), you can do:

    lein bench perf_diff_experiment -b 10 -- -b 20 -- -b 40 -- -b 80 -- -b 160 -- -b 320 -- -b 640

And it will generate lots of data and analysis Excel workbooks.

If you'd like to see the options for the benchmarking tool, just run `lein bench`.

## Technical details

See the `doc/` folder for technical details of the hitchhiker tree and Redis garbage collection system.

## License

Copyright © 2016 David Greenberg

Distributed under the Eclipse Public License version 1.0
