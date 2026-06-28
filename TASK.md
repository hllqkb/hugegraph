## [Improve] Gremlin Task Large Result Chunking

### Before submit
- [x] I have confirmed and searched that there are no similar problems in the historical issue and documents

---

### Environment

- HugeGraph version: 1.8.0+
- Java version: 11+
- Module: `hugegraph-core` (task framework), `hugegraph-api` (REST), `hugegraph-test`
- Related PR: #3060 (task metadata reads + `with_result=false`)

---

### Problem Description

When a Gremlin async task returns large results (e.g., a `g.V().valueMap()` query on a graph with millions of vertices), the entire result set is JSON-serialized, compressed, and stored as a **single** vertex property (`~task_result`). This has three compounding bottlenecks:

| Bottleneck | Code location | Current limit |
|---|---|---|
| Result item count | `GremlinJob.TASK_RESULTS_MAX_SIZE` | 800,000 (hardcoded to `Query.DEFAULT_CAPACITY`) |
| Compressed storage size | `CoreOptions.TASK_RESULT_SIZE_LIMIT` | 16 MB (default) |
| Single property maximum | `BytesBuffer.BYTES_LEN_MAX` | 10 MB |

**Current behavior (GremlinJob.execute):**
```java
List<Object> results = new ArrayList<>();
while (traversal.hasNext()) {
    results.add(traversal.next());    // all in memory, up to 800K entries
    checkResultsSize(results);
}
return results;  // full list -> HugeTask.set() -> JSON serialize -> compress -> single ~task_result
```

**Actual problems:**

1. A query returning 500K vertices with 10 properties each produces a ~50MB JSON string. `JsonUtil.toJson(result)` serializes it all in memory. If the compressed result exceeds `TASK_RESULT_SIZE_LIMIT` (16MB default), a `LimitExceedException` is thrown and the task fails.

2. On `GET /tasks/{id}?with_result=true`, the entire `~task_result` blob is decompressed and deserialized. For a 50MB result, this takes seconds and can cause timeout on slow connections, especially with the RocksDB backend where the blob is read through multiple serialization layers.

3. Users cannot incrementally consume large results. The only option is to increase `task.result_size_limit` (up to 1GB) and accept the memory/performance cost.

**Expected behavior:**

1. Large task results are stored in configurable-size chunks (default 1 MB), each stored as a separate vertex property (`~task_result_0`, `~task_result_1`, ...).
2. The REST API supports paginated retrieval: `GET /tasks/{id}?with_result=true&page=0&page_size=1000`.
3. Small results (< chunk threshold) continue to use the existing single-property path — fully backward compatible.

---

### Root Cause Analysis

The root cause is in `HugeTask.asArray()` (line 568) and `HugeTask.set()` (line 372):

```java
// HugeTask.set() - result is set as a single string
protected void set(V v) {
    String result = JsonUtil.toJson(v);          // full JSON in one string
    checkPropertySize(result, P.RESULT);         // 16MB check
    this.result = result;
    super.set(v);
}

// HugeTask.asArray() - stored as a single vertex property
if (this.result != null) {
    byte[] bytes = StringEncoding.compress(this.result);  // single blob
    checkPropertySize(bytes.length, P.RESULT);
    list.add(P.RESULT);
    list.add(bytes);
}
```

The task vertex property model already supports multiple properties (it uses a `List<Object>` key-value pair list). The limitation is purely in the serialization code — there is no splitting logic. PR #3060 added `asArrayWithoutResult()` for the metadata-only path but left the result storage unchanged.

---

### Proposed Solution

**Phase 1: Chunked Storage**

When `this.result` (the JSON string) exceeds a configurable threshold `task.result_chunk_size` (default: 1 MB), split it into chunks and store as `~task_result_0`, `~task_result_1`, ... instead of a single `~task_result`.

**Storage model:**
```
Small result (<= 1 MB):
  ~task_result = <compressed full result>           // UNCHANGED

Large result (> 1 MB):
  ~task_result_0 = <compressed chunk 0>             // NEW
  ~task_result_1 = <compressed chunk 1>
  ~task_result_2 = <compressed chunk 2>
  ~task_result_n = <chunk_count>                    // metadata marker
```

**Phase 2: Paginated API**

Add `page` and `page_size` query parameters to `GET /tasks/{id}`:
```
GET /tasks/{id}?with_result=true&page=0&page_size=1000
```
Response includes pagination metadata (`page`, `page_size`, `total`). When `page_size` is absent, the full result is returned (reassembled from chunks if needed) — backward compatible.

**Phase 3 (Future): Streaming Write**

Modify `GremlinJob.execute()` to write results in batches to avoid holding the full list in memory. Deferred to keep this task scoped.

**Key design constraints:**
1. Backward compatibility: existing single-property tasks continue to work
2. Cross-backend: all changes at the vertex property abstraction level — works across RocksDB, MySQL, PostgreSQL, Cassandra, HBase
3. JSON-level chunking: split at JSON array element boundaries, not arbitrary byte offsets

### Files to Modify

| File | LOC | Changes |
|---|---|---|
| `hugegraph-core/.../task/HugeTask.java` | 873 | Chunked property read/write, reassembly, pagination |
| `hugegraph-api/.../api/job/TaskAPI.java` | 222 | `page`, `page_size` query params, new response format |
| `hugegraph-core/.../config/CoreOptions.java` | 742 | New config: `task.result_chunk_size` (1 MB default) |
| `hugegraph-test/.../core/TaskCoreTest.java` | 816 | Unit tests: chunked storage, reassembly, pagination, backward compat |
| `hugegraph-test/.../api/TaskApiTest.java` | 189 | API integration tests for pagination |

---

### Impact

```
BEFORE - Large Gremlin task result (> 16MB):
  T=0   GremlinJob loads all results into ArrayList
  T=1   JsonUtil.toJson(result) fails or exceeds 16MB limit
  T=2   Task status: FAILED / "Task result size exceeded limit"
  User sees: 500 Internal Server Error

AFTER - Large Gremlin task result (> 16MB):
  T=0   GremlinJob loads results (unchanged for Phase 1-2)
  T=1   JsonUtil.toJson(result) succeeds
  T=2   HugeTask.asArray() splits result into 1MB chunks
  T=3   Stored as ~task_result_0 ... ~task_result_N
  User sees: GET /tasks/{id}?page=0&page_size=1000 -> first 1000 items
```

---

### Related

| Issue/PR | Description |
|---|---|
| #3060 | Added `with_result=false` + metadata-only task reads (this task builds on it) |
| #3049 | Made serializer buffer capacity configurable (orthogonal) |
| #2764 | Good First Issue list (self-proposed task model) |

---

### Implementation Plan & Progress

Implementation Plan
Breaking the work into independently reviewable and testable chunks. Each chunk has its own test, can be reviewed in isolation, and builds on the previous one.

---

**Chunk 1 — Add task.result_chunk_size config option**

File: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/config/CoreOptions.java`

Problem: No configuration exists to control result chunking behavior. The system always serializes and stores the entire task result as a single vertex property regardless of size.

Fix: Add `TASK_RESULT_CHUNK_SIZE` static option with default `1048576` (1 MB), range `[0, Bytes.GB]`. Value `0` disables chunking entirely (preserves current behavior). Register the option in `registerOptions()`. The existing `TASK_RESULT_SIZE_LIMIT` (16 MB default) remains as the hard upper bound — chunking operates within this limit to avoid hitting per-property storage ceilings.

```java
public static final ConfigOption<Long> TASK_RESULT_CHUNK_SIZE =
    ConfigOption.builder("task.result_chunk_size")
                .description("Max size in bytes per result chunk. " +
                             "0 disables chunking. Default 1MB.")
                .range(0L, Bytes.GB)
                .defaultValue(Bytes.MB)
                .build();
```

Test: Verify option is registered and default value is 1 MB. Verify range validation rejects negative values and values > 1 GB.

---

**Chunk 2 — Add property name helpers to HugeTask**

File: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/task/HugeTask.java`

Problem: There is no infrastructure for naming, detecting, or counting chunked properties. All result properties are currently accessed via a single `P.RESULT` key.

Fix: Add three static helper methods:
- `chunkKey(int index)` — returns `"~task_result_" + index` for chunk properties; returns `P.RESULT` (`"~task_result"`) for `index < 0` (the legacy key)
- `isChunkedProperty(String key)` — returns `true` if key matches `~task_result_` followed by one or more digits
- `chunkCountKey()` — returns `"~task_result_count"`, a dedicated metadata property storing the number of chunks

No behavior change — these are pure helpers used by subsequent chunks.

Test: Verify key generation: `chunkKey(0)` → `"~task_result_0"`, `chunkKey(-1)` → `"~task_result"`. Verify `isChunkedProperty("~task_result_0")` → `true`, `isChunkedProperty("~task_result")` → `false`, `isChunkedProperty("~task_name")` → `false`.

---

**Chunk 3 — Implement chunked write in HugeTask.asArray()**

File: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/task/HugeTask.java`

Problem: `HugeTask.asArray()` (line 568) always writes the entire compressed result as a single `~task_result` vertex property, with no mechanism to split large results.

Fix: After compressing `this.result` into `byte[] bytes`, check if chunking is enabled (`task.result_chunk_size > 0`) and the compressed size exceeds the threshold. If so:
1. Decompress `bytes` back to the JSON string
2. Parse the JSON string. If it's a JSON array, split along element boundaries so each chunk stays under the threshold without cutting elements in half. If it's a single object, split at byte boundaries.
3. Compress each chunk individually
4. Write `~task_result_0`, `~task_result_1`, ... `~task_result_N` to the property list
5. Write `~task_result_count = N + 1` as a metadata marker

If chunking is disabled or the result is below the threshold, the original single-property path runs unchanged.

```java
if (this.result != null) {
    byte[] bytes = StringEncoding.compress(this.result);
    long chunkSize = this.config.get(CoreOptions.TASK_RESULT_CHUNK_SIZE);
    
    if (chunkSize > 0 && bytes.length > chunkSize) {
        // Chunked path
        List<String> chunks = splitJsonArray(this.result, chunkSize);
        for (int i = 0; i < chunks.size(); i++) {
            list.add(chunkKey(i));
            list.add(StringEncoding.compress(chunks.get(i)));
        }
        list.add(chunkCountKey());
        list.add(chunks.size());
    } else {
        // Original single-property path
        list.add(P.RESULT);
        list.add(bytes);
    }
}
```

Test: Create a task with result string > 1 MB. Verify vertex has `~task_result_0` through `~task_result_N` properties plus `~task_result_count`. Verify small result (< 1 MB) still uses single `~task_result` property.

---

**Chunk 4 — Implement chunked read in HugeTask.property()**

File: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/task/HugeTask.java`

Problem: `HugeTask.property()` (line 463) only handles the single `P.RESULT` key. There is no logic to detect and reassemble chunked properties when loading a task from the backend.

Fix: In `property(String key, Object value)`:
1. When `key` matches `isChunkedProperty(key)`, collect the chunk value into a `List<String>` buffer (sorted by chunk index). 
2. When `key` matches `chunkCountKey()`, record the expected chunk count.
3. When all expected chunks have arrived (buffer size == chunk count), sort by index, decompress each, concatenate into the full JSON string, set `this.result`.
4. When `key.equals(P.RESULT)`, handle as before (legacy single-property path).
5. When `key` is not a chunk or result key, delegate to the existing property handling.

Test: Verify chunked task can be loaded from backend — `this.result` is correctly reassembled. Verify legacy single-property task still loads correctly. Verify partial chunk load (e.g., task still being written) does not produce corrupted result.

---

**Chunk 5 — Update HugeTask.asMap() for chunked/legacy compatibility**

File: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/task/HugeTask.java`

Problem: `HugeTask.asMap()` (line 628) calls `this.result()` to get the stored result string. This path must work identically whether the underlying storage is chunked or legacy single-property.

Fix: No code change needed here if Chunk 4 correctly restores `this.result` during `property()` processing. The `asMap()` method's existing `this.result()` call will return the full reassembled result. Add a guard: if chunked result is being reassembled (buffer not yet complete), `this.result()` should return `null` until all chunks are loaded.

This is primarily a verification chunk: confirm the existing `asMap()` logic works through the chunked read path from Chunk 4.

Test: Verify `asMap(true, true)` returns full result for chunked task. Verify `asMap(true, false)` (the `with_result=false` path from #3060) returns metadata without result for chunked task. Verify `asMap()` with legacy single-property task still works.

---

**Chunk 6 — Add unit tests for chunked storage**

File: `hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/core/TaskCoreTest.java`

Problem: No test coverage for chunked result storage, reassembly, or backward compatibility.

Fix: Add four test methods:
- `testTaskResultChunked()` — create task with result JSON string > 1 MB. Verify task vertex stores `~task_result_0`, `~task_result_1`, ... plus `~task_result_count`. Verify `task.result()` returns the full reassembled result.
- `testTaskResultSmall()` — create task with result < 1 MB. Verify task vertex stores single `~task_result` property (backward compat). Verify `task.result()` returns correct value.
- `testTaskResultChunkReassembly()` — create task with known result, verify reassembled result equals original exactly (byte-level).
- `testTaskResultBackwardCompat()` — directly construct a task vertex with legacy single `~task_result` property. Load it. Verify `task.result()` works.
- `testTaskResultChunkDisabled()` — set `task.result_chunk_size` to `0`. Create large task. Verify single-property path used.

Test data: Generate result strings of known sizes using a helper method that creates a JSON array with N elements, e.g., `["result-item-00000", "result-item-00001", ...]`.

---

**Chunk 7 — Add pagination fields and logic to HugeTask**

File: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/task/HugeTask.java`

Problem: `HugeTask` has no concept of pagination. The entire result is always returned.

Fix: Add two transient fields: `private int page = -1` and `private int pageSize = -1`. When both are `>= 0`, `asMap(true, true)` should apply pagination:
1. Parse `this.result` into a list of objects (the Gremlin result is always a list)
2. Compute `start = page * pageSize`, `end = Math.min(start + pageSize, list.size())`
3. Return `list.subList(start, end)` as the result
4. Include a `pagination` metadata map: `{"page": page, "page_size": pageSize, "total": list.size()}`
5. When both fields are `-1` (default), return the full result — backward compatible

6. Add getter/setter for `page` and `pageSize`.

Test: Verify pagination with known list size: page=0, pageSize=10 returns first 10 items. page=2, pageSize=10 returns items 20-29. Verify metadata shows correct total. Verify page=-1 returns all items.

---

**Chunk 8 — Add page and page_size query params to TaskAPI.get()**

File: `hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/job/TaskAPI.java`

Problem: `GET /tasks/{id}` has no pagination query parameters.

Fix: Add two new `@QueryParam` annotations to the existing `get()` method:
```java
@QueryParam("page") @DefaultValue("-1") int page,
@QueryParam("page_size") @DefaultValue("-1") int pageSize
```
When both `page >= 0` and `pageSize >= 0`, pass them to the task before calling `asMap()`:
```java
HugeTask<?> task = scheduler.task(IdGenerator.of(id), withResult);
if (page >= 0 && pageSize >= 0) {
    task.page(page);
    task.pageSize(pageSize);
}
return task.asMap(true, withResult);
```
When either is `-1` (default), behavior is unchanged — full result returned.

Test: Verify `GET /tasks/{id}?with_result=true&page=0&page_size=10` returns first page. Verify `GET /tasks/{id}?with_result=true` (no pagination params) returns full result. Verify `GET /tasks/{id}?with_result=false&page=0&page_size=10` does not include result (metadata-only takes precedence).

---

**Chunk 9 — Add task(Id, boolean, int, int) overload to TaskScheduler**

File: `hugegraph-server/hugegraph-core/src/main/java/org/apache/hugegraph/task/TaskScheduler.java` (interface) + `StandardTaskScheduler.java` (implementation)

Problem: The `TaskScheduler.task(Id, boolean withResult)` method has no way to pass pagination parameters to the task. The current workaround uses setters on the returned task object, but this creates a race condition if the task's `asMap()` is called before the setters execute.

Fix: Add a new overload:
```java
// Interface
<V> HugeTask<V> task(Id id, boolean withResult, int page, int pageSize);

// Implementation (StandardTaskScheduler)
@Override
public <V> HugeTask<V> task(Id id, boolean withResult, int page, int pageSize) {
    HugeTask<V> task = this.task(id, withResult);
    if (page >= 0 && pageSize >= 0) {
        task.page(page);
        task.pageSize(pageSize);
    }
    return task;
}
```
Update `TaskAPI.get()` (Chunk 8) to use this overload instead of manual setter calls.

Test: Verify `task(123, true, 0, 10).asMap(true, true)` returns first 10 results with pagination metadata.

---

**Chunk 10 — Add API integration tests for pagination**

File: `hugegraph-server/hugegraph-test/src/main/java/org/apache/hugegraph/api/TaskApiTest.java`

Problem: No test coverage for paginated result retrieval via REST API.

Fix: Add three test methods:
- `testGetWithPagination()` — create a Gremlin task that returns 100 items. Call `GET /tasks/{id}?with_result=true&page=0&page_size=30`. Assert 30 items returned. Call `page=3&page_size=30`. Assert 10 items returned (last page has remainder). 
- `testGetPaginationMetadata()` — create task with known result. Verify response includes `pagination` object with correct `total`, `page`, `page_size`.
- `testGetWithoutPagination()` — create task with pagination-capable result. Call without `page`/`page_size` params. Assert full result returned (backward compat — no pagination object present).
- `testGetInvalidPage()` — call with `page=-1&page_size=10`. Verify full result returned (graceful fallback).

---

**Chunk 11 — Manual verification: chunked storage**

Manual test procedure (no automated test — verifies the full end-to-end storage path):

1. Start HugeGraph server with RocksDB backend
2. Create a schema and insert test data (e.g., 50K vertices with labels)
3. Submit a Gremlin async task: `g.V().hasLabel("test").valueMap()`
4. Wait for task to SUCCESS
5. Inspect task vertex properties via `GET /graphs/hugegraph/graph/vertices/{task_id}` (raw vertex API or debug log)
6. Verify `~task_result_0`, `~task_result_1`, ... `~task_result_N` exist (if > 1 MB) or single `~task_result` exists (if ≤ 1 MB)
7. Verify `~task_result_count` = N + 1 (if chunked)
8. Run `GET /tasks/{id}?with_result=true` and verify the full result equals the query result

---

**Chunk 12 — Manual verification: paginated REST API**

Manual test procedure:

1. Use the task from Chunk 11 (with chunked result)
2. `GET /tasks/{id}?with_result=true&page=0&page_size=100` → verify first 100 items
3. `GET /tasks/{id}?with_result=true&page=1&page_size=100` → verify items 100-199 (different from page 0)
4. `GET /tasks/{id}?with_result=true&page=999&page_size=100` → verify empty array returned with correct `total`
5. `GET /tasks/{id}?with_result=true` (no pagination) → verify full result, no `pagination` key in response
6. Verify each page response includes `pagination: { page, page_size, total }`

---

**Chunk 13 — Run full test suite**

Commands:
```bash
# Core tests (task framework)
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb

# API tests (REST endpoints)
mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,rocksdb

# Full test suite
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,api-test,rocksdb
```

Expected: All existing tests pass. New tests from Chunk 6 and Chunk 10 pass. No regressions in unrelated tests.
