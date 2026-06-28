# Proposal: Gremlin Task Large Result Chunking

## Summary

When a Gremlin async task returns large results, the entire result set is JSON-serialized, compressed, and stored as a single vertex property (`~task_result`). This has three bottlenecks: memory (full ArrayList in GremlinJob), storage (16 MB single-property limit), and API retrieval (full decompress + deserialize on every `GET /tasks/{id}`). This proposal introduces chunked storage and paginated retrieval for large Gremlin task results.

## Motivation

PR [#3060] (merged) added metadata-only task reads and `with_result=false` support, making task listing and deletion usable when large historical results exist. That PR explicitly deferred result chunking:

> *"large Gremlin result export/chunking can be handled separately"*

This proposal is the natural follow-up: handle the case where the user *does* want the result, but the result is too large to serve as a single blob.

### Current result data flow

```
GremlinJob.execute()
  → List<Object> results = new ArrayList<>()    // all in memory, 800K cap
  → return results                               // full list
      ↓
HugeTask.set(v)   [line 372]
  → String result = JsonUtil.toJson(v)           // full JSON serialization
  → checkPropertySize(result, P.RESULT)          // 16 MB default limit
  → this.result = result                         // stored as volatile String
      ↓
HugeTask.asArray()   [line 568]
  → byte[] bytes = StringEncoding.compress(result)   // compress
  → checkPropertySize(bytes.length, P.RESULT)        // check again
  → list.add(P.RESULT); list.add(bytes)              // single vertex property
      ↓
StandardTaskScheduler.save()   [line 464]
  → constructVertex(task) → addVertex(vertex)    // written to backend store
      ↓
GET /tasks/{id}?with_result=true   [TaskAPI line 141]
  → full decompress + deserialize → return entire JSON
```

### Bottlenecks

| Bottleneck | Code location | Current limit |
|---|---|---|
| Result item count | `GremlinJob.TASK_RESULTS_MAX_SIZE` | `Query.DEFAULT_CAPACITY` = 800,000 |
| Compressed storage | `CoreOptions.TASK_RESULT_SIZE_LIMIT` | 16 MB (default), 1 GB (max) |
| Single property maximum | `BytesBuffer.BYTES_LEN_MAX` | 10 MB |

## Proposed Solution

### Phase 1: Chunked storage (backward compatible)

When the JSON-serialized result exceeds a configurable threshold (default: 1 MB), split it into chunks and store them as separate vertex properties instead of a single `~task_result`.

**Storage model:**

```
Small result (≤ threshold):
  ~task_result = <compressed full result>           // unchanged, backward compat

Large result (> threshold):
  ~task_result_0   = <compressed chunk 0>           // new
  ~task_result_1   = <compressed chunk 1>
  ~task_result_2   = <compressed chunk 2>
  ...
  ~task_result_n   = <chunk_count>                  // metadata marker
```

The property naming uses sequential numeric suffixes. The chunk count is stored in the last chunk as a marker, so the reader knows how many chunks to expect without scanning all properties.

### Phase 2: Paginated API retrieval

```
GET /tasks/{id}?with_result=true&page=0&page_size=1000
```

New query parameters:
- `page` (default: 0) — which page of results to return
- `page_size` (default: null, meaning "all") — items per page

Response for paginated results:
```json
{
  "id": 123,
  "type": "gremlin",
  "status": "success",
  "task_result": [ ... items for page 0 ... ],
  "pagination": {
    "page": 0,
    "page_size": 1000,
    "total": 50000
  }
}
```

When `page_size` is absent, behavior is unchanged — return the full result (reassembled from chunks if needed).

### Phase 3 (future): Streaming write from GremlinJob

Modify `GremlinJob.execute()` to write results to `HugeTask` in batches instead of holding the entire list in memory. This removes the 800K item count limit and the memory bottleneck. Deferred to a follow-up to keep this proposal scoped.

## Implementation Plan

### Files to modify

| File | LOC | Changes |
|---|---|---|
| `hugegraph-core/.../task/HugeTask.java` | 873 | Chunked property names, split logic in `asArray()`, reassembly in `property()`, pagination in `asMap()` |
| `hugegraph-api/.../api/job/TaskAPI.java` | 222 | `page` and `page_size` query params, response format |
| `hugegraph-core/.../config/CoreOptions.java` | 742 | New config: `task.result_chunk_size` |
| `hugegraph-test/.../core/TaskCoreTest.java` | 816 | Tests for chunked storage and paginated retrieval |
| `hugegraph-test/.../api/TaskApiTest.java` | 189 | API pagination tests |

### Key design decisions

1. **Backward compatibility is mandatory.** Existing stored tasks with single `~task_result` must continue to work. The chunked format is only used for new saves.

2. **Chunk size is configurable.** `task.result_chunk_size` with default 1 MB (1,048,576 bytes). This keeps individual vertex properties small while not generating excessive property counts.

3. **JSON-level chunking, not byte-level.** Chunks are split at the JSON array boundary (item-level), not at arbitrary byte offsets. This ensures each chunk is valid JSON and can be independently parsed.

4. **No change to GremlinJob execution.** Phase 1-2 only changes storage and retrieval. The execution path (loading results into ArrayList) is unchanged for now, keeping risk low.

5. **No new storage schema.** Chunks use the existing vertex property mechanism. No new tables, no migration scripts needed.

### Chunk split algorithm

```java
// In HugeTask.asArray(), after JSON serialization:
String resultJson = this.result;  // the JSON-serialized result list
if (resultJson.length() > chunkSizeThreshold) {
    // Split the result into chunks
    // For array results: split at top-level array element boundaries
    // For single-object results: split raw bytes at chunk boundaries
    List<String> chunks = splitResult(resultJson, chunkSizeThreshold);
    for (int i = 0; i < chunks.size(); i++) {
        byte[] compressed = StringEncoding.compress(chunks.get(i));
        list.add("~task_result_" + i);
        list.add(compressed);
    }
} else {
    // Original single-property path (unchanged)
    list.add(P.RESULT);
    list.add(StringEncoding.compress(this.result));
}
```

### Pagination algorithm

```java
// In HugeTask.asMap(withResult=true, page=0, pageSize=1000):
List<Object> allResults;
if (isChunked(vertex)) {
    allResults = reassembleChunkedResult(vertex);
} else {
    allResults = parseSingleResult(vertex);
}
// Apply pagination
int start = page * pageSize;
int end = Math.min(start + pageSize, allResults.size());
return ImmutableMap.of(
    "task_result", allResults.subList(start, end),
    "pagination", ImmutableMap.of(
        "page", page, "page_size", pageSize, "total", allResults.size()
    )
);
```

## Testing Strategy

### Unit tests (TaskCoreTest)

1. `testTaskResultChunked()` — verify result is stored as multiple properties when > threshold
2. `testTaskResultSmall()` — verify small results still use single property
3. `testTaskResultChunkReassembly()` — verify chunks are correctly reassembled
4. `testTaskResultPagination()` — verify page/page_size works with chunked results
5. `testTaskResultBackwardCompat()` — verify old single-property tasks still work
6. `testTaskResultSameAsBefore()` — verify reassembled chunked result equals original

### API tests (TaskApiTest)

1. `testGetWithPagination()` — verify `page` and `page_size` query params
2. `testGetPaginationMetadata()` — verify `total`, `page_size` in response
3. `testGetWithoutPagination()` — verify backward compat (no page params = full result)

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Breaking existing stored tasks | Single-property path unchanged; chunk detection is additive |
| Property count explosion | 1 MB chunks → max ~16 chunks at 16 MB limit; well under vertex property limits (UINT16_MAX) |
| Cross-backend compatibility | All changes at HugeTask level, which uses vertex property abstraction; no backend-specific code |
| Test flakiness | Use fixed-size test data; not dependent on timing or concurrency |

## Relationship to Existing Work

- **PR #3060**: Added `with_result=false` and metadata-only reads. This proposal builds on it by adding chunked storage for the `with_result=true` path.
- **PR #3049**: Made serializer buffer capacity configurable. Orthogonal — addresses serialization buffer, not result storage.

## Deliverables

1. Implementation in `HugeTask.java`, `TaskAPI.java`, `CoreOptions.java`
2. Unit tests in `TaskCoreTest.java`
3. API integration tests in `TaskApiTest.java`
4. Feature verification: create Gremlin task with large result, verify chunked storage, verify paginated retrieval, verify small-result backward compatibility
