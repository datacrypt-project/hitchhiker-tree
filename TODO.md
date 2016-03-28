Comparative benchmark modes--apples to apples (by params) and everything vs. one trial
This will help guide optimization work

Figure out where the huge spikes in operation time come from (tiered resizing?)

We need a more thorough set of tests for the messaging system to saturate deletes, to ensure we don't have more lurking bugs

Implementations of backwards scan, pred, and succ

Write the WAL, back with in-mem and redis, then add to benchmarks. This is necessary for disk-backed work

Choose splits and sizes based on serialized results--big perf gain

benchmark dataset like (map + (repeatedly rand) (iterate #(+ 0.01 %) 0.0))
that's a sliding random window, for some random moving write heavy region
with lots of cold nodes

Add async writes

Add support for datascript
