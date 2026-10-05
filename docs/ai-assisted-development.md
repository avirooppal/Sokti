# Engineering Realities in AI-Assisted Data Platform Development

This report documents the concrete failure modes, hallucinations, unworkable defaults, and domain-specific subtleties encountered when leveraging Large Language Models to build **Sokti**. It contrasts naive code generation with the rigorous systems engineering required to deliver a reliable, production-ready platform.

---

## 1. Executive Summary

While LLMs excel at generating boilerplate scaffolds and syntactically plausible snippets, naive AI output routinely fails when applied to distributed data systems. The primary failure patterns observed include:
1. **Version Drift & Deprecated Driver Settings:** Generating code compatible with older database releases that crash on newer versions.
2. **Dialect Mismatches in Distributed SQL:** Conflating ANSI standard SQL with specialized OLAP engines (ClickHouse).
3. **Container Networking & DNS Idiosyncrasies:** Blind assumptions about `localhost`, IPv6 resolution, and Docker host binding.
4. **Subtle Datatype Rigidities:** Applying string null-checks (`col = ''`) against native UUID types in column-oriented stores.

Below are 6 documented case studies from this codebase.

---

## 2. Case Studies & Technical Interventions

### Case Study 1: `dbt-clickhouse` 1.10 vs ClickHouse 24.3 Driver Incompatibility
- **Naive AI Assumption:** Installing `dbt-core` and `dbt-clickhouse` and running `dbt run` would execute models cleanly.
- **The Failure:** Every single dbt invocation crashed with:
  ```
  Database Error in model stg_playback_events
  Received ClickHouse exception, code: 115, server response:
  Code: 115. DB::Exception: Unknown setting lightweight_deletes_sync. (UNKNOWN_SETTING) (version 24.3.18.7)
  ```
- **Root Cause Analysis:** `dbt-clickhouse` 1.10.3 hardcoded `self._conn_settings.setdefault('lightweight_deletes_sync', '3')` inside `dbclient.py`. In ClickHouse 24.3 LTS, this experimental setting was removed/renamed. The client driver aggressively injected an invalid setting into every HTTP connection header.
- **Production Engineering Fix:** We created an automated monkey-patch utility (`scripts/patch_dbt_clickhouse.py`) that audited the installed adapter, removed the invalid key from `self._conn_settings`, and restored compatibility:
  ```python
  # scripts/patch_dbt_clickhouse.py
  # Strips defunct 'lightweight_deletes_sync' from dbt ClickHouse driver
  conn_settings.pop('lightweight_deletes_sync', None)
  ```
  *Result:* All 13 models immediately compiled and ran successfully.

---

### Case Study 2: ClickHouse CTE Join Ambiguity & Column Scoping
- **Naive AI Assumption:** Standard ANSI SQL multi-table joins work uniformly across all relational and OLAP engines:
  ```sql
  -- Naive AI generated SQL in mart_churn_features.sql
  SELECT w.user_id, w.genre_watch_minutes_7d, s.search_count_7d
  FROM user_watch_stats w
  LEFT JOIN user_searches s ON w.user_id = s.user_id
  LEFT JOIN user_recs r ON w.user_id = r.user_id
  ```
- **The Failure:** ClickHouse 24.3 threw:
  ```
  Code: 47. DB::Exception: Missing columns: 'user_id' while processing query: 'user_id', required columns: 'user_id' 'user_id'. (UNKNOWN_IDENTIFIER)
  ```
- **Root Cause Analysis:** ClickHouse processes chained joins as binary steps `((w LEFT JOIN s) LEFT JOIN r)`. When table aliases are chained without explicit projection, alias scoping creates ambiguity in the query planner.
- **Production Engineering Fix:** Refactored the query to use ClickHouse's native `USING (user_id)` syntax:
  ```sql
  -- Production Refactored SQL
  SELECT
      user_id,
      w.genre_watch_minutes_7d,
      coalesce(s.search_count_7d, 0) AS search_count_7d
  FROM user_watch_stats w
  LEFT JOIN user_searches s USING (user_id)
  LEFT JOIN user_recs r USING (user_id)
  ```
  *Result:* Query planned without ambiguity, executing in under 0.5 seconds across all cohorts.

---

### Case Study 3: Scalar vs Aggregate Function Semantics (`max` vs `greatest`)
- **Naive AI Assumption:** Using `max(expression, 1)` to prevent division-by-zero errors in analytical calculations:
  ```sql
  round(countIf(event_type = 'recommendation_click') / max(count(*), 1) * 100.0, 2)
  ```
- **The Failure:** ClickHouse threw a function signature mismatch error.
- **Root Cause Analysis:** In ClickHouse, `max()` is strictly an aggregate function operating on a column across rows. In contrast, evaluating the maximum of two scalar values requires `greatest(a, b)`.
- **Production Engineering Fix:** Replaced `max(count(*), 1)` with `greatest(count(*), 1)`.

---

### Case Study 4: Alpine Linux IPv6 DNS Resolution in Container Health Checks
- **Naive AI Assumption:** Standard Docker Compose health check `wget http://localhost:8123/ping` would confirm ClickHouse health.
- **The Failure:** ClickHouse container was perpetually stuck in `(unhealthy)`, preventing downstream containers (`dbt`, `kafka-connect`) from starting.
- **Root Cause Analysis:** Inside the minimal Alpine image used by `clickhouse-server:24.3-alpine`, `localhost` resolved first to IPv6 `::1`. ClickHouse was bound to IPv4 `0.0.0.0:8123`, rejecting IPv6 connection attempts with connection refused.
- **Production Engineering Fix:** Explicitly pinned the healthcheck probe to the IPv4 loopback interface:
  ```yaml
  healthcheck:
    test: ["CMD-SHELL", "wget --no-verbose --tries=1 --spider http://127.0.0.1:8123/ping || exit 1"]
  ```

---

### Case Study 5: MongoDB Text Index Language Override on Global Content
- **Naive AI Assumption:** Creating a standard MongoDB text index over OTT metadata:
  ```python
  db.movies.create_index([("title", "text"), ("synopsis", "text")])
  ```
- **The Failure:** Bulk seeding failed with:
  ```
  pymongo.errors.BulkWriteError: language override unsupported: ja
  ```
- **Root Cause Analysis:** Synthetic OTT media documents included Japanese content with `"language": "ja"`. MongoDB text indexes by default inspect a field named `language` in documents to apply language-specific stemmers. Because MongoDB text search does not provide native Japanese stemming out of the box, it aborted the write.
- **Production Engineering Fix:** Explicitly disabled document language overrides during index definition:
  ```python
  db.movies.create_index(
      [("title", "text"), ("synopsis", "text")],
      name="content_text_search_idx",
      default_language="english",
      language_override="none"
  )
  ```

---

### Case Study 6: Strongly Typed UUID Representation in ClickHouse
- **Naive AI Assumption:** In data quality checks, checking for missing or empty keys using typical SQL idioms:
  ```sql
  countIf(session_id IS NULL OR session_id = '')
  ```
- **The Failure:** Query aborted with:
  ```
  Code: 376. DB::Exception: Cannot parse uuid : while converting '' to UUID. (CANNOT_PARSE_UUID)
  ```
- **Root Cause Analysis:** ClickHouse column `session_id` is defined as `UUID` (non-nullable 128-bit value). ClickHouse performs compile-time type coercion: attempting to evaluate `session_id = ''` forces ClickHouse to parse `''` as a UUID, triggering an immediate parse exception.
- **Production Engineering Fix:** Leveraged native ClickHouse UUID representation:
  ```sql
  countIf(session_id = toUUID('00000000-0000-0000-0000-000000000000'))
  ```

---

## 3. Naive Code Generation vs Production Engineering

| Dimension | Naive LLM Generation | Production Engineering Practice |
|---|---|---|
| **Error Handling** | Generic `try / except Exception: pass` | Dead-letter queue isolation, structured alert payloads, retry with exponential backoff |
| **Data Quality** | Implicit assumption that data is clean | Multi-layered validation: Schema Registry, real-time watermarks, ClickHouse quarantine tables |
| **Stateful Processing** | Stateless scripts processing single events | Watermarked tumbling windows, LRU event_id deduplication, sessionization |
| **Privacy / PII** | Raw emails and phone numbers propagated everywhere | Role-based masking, pseudonymization, right-to-be-forgotten deletion workflows |
| **Port & Host Clashes** | Hardcoded standard ports (5432, 27017) crashing on host | Fully configurable `.env` port mappings with network isolation |
