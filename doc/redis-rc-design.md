# Refcounting (RC) System for Redis

One challenge in storage large amounts of off-heap data is deciding when data is old enough to delete.
The hitchhiker tree is a functional data structure, which makes use of a variation of the classic technique of path-copying to reduce IO on updates.
Unfortunately, path-copying relies on a garbage collector;
thus, the need for this system.

The system is written primarily as Lua scripts for Redis to ensure atomicity and portability.
Since Redis has no API to set events in the future, a small amount of code must run on the client.
Luckily, this code simple runs a Lua a script, sleeps for the amount of time returned by the script, and repeats this in a loop.

## System Requirements and Limitation

Importantly, this system currently doesn't support any method of cycle-detection or cycle-breaking.
If you store a reference to a value without updating the bookkeeping structures,
that value is eligible for collection.

Collections are only triggered by the action of dropping a reference.

In order to simplify the avoidance of memory leaks, the RC system also can be configured
to automatically expire old data.
Currently, data expires by default after 5 seconds, which is hardcoded in the `hitchhiker.redis.RedisBackend` implementation of `anchor-root`.

## Design

Implicitly, for this garbage collector to work, we assume all keys are immutable.
It may be possible to allow mutation in the future, but the design doesn't currently support it.

We allow each key to refer to however many other keys it needs to keep alive.
When a key has no more references, it is freed, along with anything it was keeping alive.

This system not only supports explicitly allocated & freed garbage collection roots (i.e. named values), it also supports implicitly expiring roots.
Automatically expiring are great for garbage collected languages, when there's no guarantee of when a finalizer will run.
By having the primitive of a root which expires, we can avoid memory leaks due to clients not explicitly freeing snapshots, which makes analytics simpler to write.

### Details

Each refcounted key gets 2 extra auxiliary keys.
For the purposes of this discussion, we'll discuss a key called `key1`, `key2`, etc.

For `key1`, we also have `key1:rc` and `key1:rl`.
The `...:rc` auxiliary stores a count of the number of references pointing to this key.
The `...:rl` auxiliary stores a list of the target keys that this key has references to.

When we want to store a new refcounted value (say, `key2`), we store its data in `key2`.
Then, we store the names of any values it refers to in `key2:rl`, a Redis list.
Suppose that `key2` refers to `key1`.
Finally, we'd increment `key2:rc`, since there's an additional reference to it.
This operation can be batched via `hitchhiker.redis/add-refs`, which takes a new value and the names of every key it points to.

We also have a helper Lua function called `drop_ref` (located in `hitchhiker.redis/drop-ref-lua`).
When `drop_ref` is called on a key, it attempts to decrement that key's `...:rc`.
If the `...:rc` reaches zero, it follows all the links in `...:rl` and recursively drops their refs.
Note that this functionality is actually implemented iteratively and incrementally to ensure deletions are efficient and scalable.
The function `hitchhiker.redis/drop-ref` exposes this.

We also store a zset of keys sorted by expiration time in the redis key `refcount:expiry`.
This zset enables the client to find out how long to sleep until the next expiring key will be ready, and then automatically expire & GC any old data.
This functionality is implemented by the `hitchhiker.redis/get-next-expiry` function, which is called in a loop by the `start-expiry-thread!`.

The current design isn't fully atomic, but it is conservatively written such that small amounts of data could leak during a crash, but valid data will always be stored.
