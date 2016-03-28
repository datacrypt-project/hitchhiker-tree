# Refcounting (RC) System for Redis

One challenge in storage large amounts of off-heap data is deciding when data is old enough to delete.
The hitchhiker tree is a functional data structure, which makes use of a variation of the classic technique of path-copying to reduce IO on updates.
Unfortunately, path-copying relies on a garbage collector;
thus, the need for this system.

The system is written primarily as Lua scripts for Redis to ensure atomicity and portability;
the timeout mechanism has very little code, but must run on the client side due to Redis's design.

## System Requirements and Limitation

Importantly, this system currently doesn't support any method of cycle-detection or cycle-breaking.
If you store a reference to a value without updating the bookkeeping structures,
that value is eligible for collection.

Collections are only triggered by the action of dropping a reference.

In order to simplify the avoidance of memory leaks, the RC system also can be configured
to automatically expire old data.
Currently, data expires by default after 5 seconds, which is hardcoded in the `hitchhiker.redis.RedisBackend` implementation of `anchor-root`.

## Design

Each refcounted key gets 2 extra auxiliary keys.
For the purposes of this discussion, we'll assume the keys are called `key1`, `key2`, etc.

For `key1`, we also have `key1:rc` and `key1:rl`.
The `...:rc` auxiliary stores a count of the number of references pointing to this key.
The `...:rl` auxiliary stores a list of the target keys that this key has references to.
We have a helper Lua function called `drop_ref` (located in `hitchhiker.redis/drop-ref-lua`).
When `drop_ref` is called on a key, it attempts to decrement that key's `...:rc`.
If the `...:rc` reaches zero, it follows all the links in `...:rl` and recursively drops their refs.
Note that this functionality is actually implemented iteratively and incrementally to ensure deletions are efficient and scalable.

We also store a zset of keys sorted by expiration time in the redis key `refcount:expiry`.
This zset enables the client to find out how long to sleep until the next expiring key will be ready.
This functionality is implemented by the `hitchhiker.redis/get-next-expiry` function, which is called in a loop by the `start-expiry-thread!`.

The current design isn't fully atomic, but it is conservatively written such that small amounts of data could leak during a crash, but valid data will always be stored.
