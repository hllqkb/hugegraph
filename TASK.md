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
```
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

1. Large task results are stored in configurable-size chunks, each stored as a separate vertex property.
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

**Phase 1: Chunked Storage**

- [ ] **Chunk 1:** Add `task.result_chunk_size` config option to `CoreOptions.java` (default 1 MB, range 0-1 GB)
- [ ] **Chunk 2:** Implement property name helpers (`hasChunkedResult()`, `chunkKey(int)`) in `HugeTask.java`
- [ ] **Chunk 3:** Implement chunked write in `HugeTask.asArray()` — split result > threshold into `~task_result_N`
- [ ] **Chunk 4:** Implement chunked read in `HugeTask.property()` — detect and reassemble `~task_result_N`
- [ ] **Chunk 5:** Update `HugeTask.asMap()` for backward-compatible result output (both chunked and legacy)
- [ ] **Chunk 6:** Add unit tests in `TaskCoreTest.java`: chunked storage, reassembly, small result backward compat

**Phase 2: Paginated API**

- [ ] **Chunk 7:** Add `page`, `pageSize` fields to `HugeTask` and pagination logic in `asMap()`
- [ ] **Chunk 8:** Add `page`, `page_size` query params to `TaskAPI.get()`
- [ ] **Chunk 9:** Add pagination metadata (`total`, `page`, `page_size`) to API response
- [ ] **Chunk 10:** Add API integration tests in `TaskApiTest.java`

**Phase 3: Validation**

- [ ] **Chunk 11:** Manual verification: create Gremlin task with large result, verify chunked storage
- [ ] **Chunk 12:** Manual verification: paginated retrieval via REST API (`page`/`page_size`)
- [ ] **Chunk 13:** Run full test suite: `mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb`
