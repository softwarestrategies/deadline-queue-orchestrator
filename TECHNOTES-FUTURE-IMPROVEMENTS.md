# Future improvements

## Concerns Worth Addressing

- In-memory LRU cache breaks under horizontal scaling — each pod has its own cache, so duplicate Kafka messages can slip through to DB on multi-replica deployments. The DB unique constraint catches it, but adds retry overhead. Consider Redis-based deduplication.

- No exponential backoff on retries — currently just increments retryCount. Under load spikes, retries all fire ASAP and compound the problem. Recommend adding nextRetryAt with backoff (e.g. 2^retryCount * 30s).

- No circuit breaker on HTTP delivery — if a downstream webhook is unhealthy, events pile up in retry state. Resilience4j CircuitBreaker would protect the scheduler loop.

## Performance Profile

At 10M events/day (~115/sec avg, ~1000/sec peak):

- 48 Kafka partitions (prod) is appropriately sized
- reWriteBatchedInserts=true on the JDBC URL is a good catch
- HikariCP at 50–100 connections in prod is reasonable
- The @Scheduled poll at 500ms in prod could create latency jitter at high load — worth profiling with your actual event distribution






## Architecture & Scale Assessment — 10M+ Events/Day

Yes, it can handle 10M+ events/day on a single node with reasonable hardware, and it scales horizontally without coordination. The core design decisions are sound: Kafka as the ingestion buffer, SELECT FOR UPDATE SKIP LOCKED for lock-free distributed polling, monthly partitioning, virtual threads for concurrent delivery. These are the right choices. The bugs we fixed were what stood between the design and it actually working.

Here is the detailed breakdown of where it stands today, where the limits are, and what to think about as volume grows.

### What works well at scale

Kafka as ingestion buffer. The REST API→Kafka→DB two-phase design is the right call. It decouples client-facing latency (sub-millisecond Kafka produce) from DB write latency, absorbs traffic spikes without back-pressure reaching clients, and gives you natural replay on consumer failure. At 10M events/day (~116/second average, ~500-1000/s peak), a 24-partition ingestion topic with 10 consumer threads is more than enough headroom.

SELECT FOR UPDATE SKIP LOCKED. This is the correct primitive for multi-instance distributed polling. There is no coordinator, no ZooKeeper, no leader election — each instance independently races for rows, skips locked ones, and processes its share. Adding a second or third instance doubles or triples throughput with zero code changes. This is the right design.

Virtual threads for delivery. Now that the sequential loop is fixed, each batch delivers all events concurrently. A single node handling 100 events/batch at 200ms median latency processes 500 events/second — well above the sustained average. At 10M/day you need ~116/s sustained; this has ~4x headroom on a single node before you need a second instance.

Monthly partitioning. DROP TABLE scheduled_events_2024_06 is instant, whereas DELETE WHERE executed_at < cutoff on an unpartitioned 100M-row table takes minutes and generates massive WAL. Partition pruning also means the scheduler's poll query only scans the current and next partition rather than the full table. This is correct at scale.

Deduplication LRU cache. The 100K entry in-memory cache avoids a DB round-trip for hot duplicates on the same instance. The unique constraint catches cross-instance duplicates. The two-layer approach is correct.

Current limits and where they will be hit
HikariCP pool: 20 connections. The poll loop now fires 100 virtual threads concurrently, each of which calls markEventCompleted or handleDeliveryFailure — both @Transactional, both need a DB connection. 100 concurrent deliveries competing for 20 connections means 80 virtual threads are parked waiting for a connection on every batch. This is not a deadlock but it is a throughput ceiling. At 10M/day with the default batch size of 100 and poll interval of 1s, you are probably fine — in practice deliveries complete faster than all 100 would be simultaneously waiting. But at 3-4 nodes each doing 100 concurrent deliveries, the pool will saturate. The fix is to increase maximum-pool-size to match realistic concurrency — around 50-100 for a single node doing serious throughput.

Gauge queries run on every Prometheus scrape. MetricsConfig defines pendingEventsMetric and processingEventsMetric as lambdas that call repository.countByStatus() directly. Prometheus scrapes every 15 seconds by default. Each scrape fires two COUNT(*) queries against a partitioned table that may have millions of rows. At 10M events/day with a 7-day retention window that is potentially 70M rows. A COUNT(*) on a partitioned table is an aggregate across all partitions — expensive. MetricsUpdater also runs the same two queries every 5 seconds independently. This means the same COUNT(*) queries run up to every 5 seconds and on every scrape — potentially 4 queries every 5 seconds hitting a large table. The fix is to pick one update path (MetricsUpdater is better — it caches in an AtomicLong), remove the live query from the Gauge lambda, and increase the update interval to 30-60 seconds.

getStatusStatistics() for the /statistics endpoint is a full table scan. SELECT status, COUNT(*) FROM scheduled_events GROUP BY status has no WHERE clause. On a large table this touches every partition. Fine for an admin endpoint called infrequently, but worth noting — don't ever put this behind a health check or dashboard auto-refresh.

The Kafka consumer ack happens regardless of DLQ send success. In consumeIngestionBatch, ack.acknowledge() is called unconditionally at the end. If kafkaProducerService.sendToDlq() fails (Kafka is unavailable), the failed message is silently lost — offset committed, DLQ not written. At scale this is a data-loss window. It is small (DLQ send failure is rare) but worth noting.

HTTP retry sleep blocks the virtual thread. EventDeliveryService.sleep() calls Thread.sleep() with 500ms / 1000ms backoff. On a virtual thread this is fine — it yields to the carrier thread. But with 100 concurrent deliveries and a retriable 503 storm from a downstream service, you could have 100 virtual threads each sleeping for 1 second on their second retry, holding their DB locks (the event is in PROCESSING status with a 5-minute lock expiry) for the full retry duration. Not a correctness issue but it means your effective throughput during a downstream incident drops significantly.

Multi-instance behaviour
Correctly designed. SELECT FOR UPDATE SKIP LOCKED guarantees each event is processed by exactly one instance at a time. Lock expiry (5 minutes) with inline recovery means a crashed instance's events are picked up by survivors on the next poll cycle. The Kafka consumer group means ingestion partitions are automatically rebalanced across instances. No shared mutable state exists between instances — no in-process caching of events, no shared scheduler state.

One thing to watch: the PartitionMaintenanceService (newly added) runs on every instance. If you have 3 instances, the weekly job fires 3 times. The DB function is idempotent (IF NOT EXISTS) so this is harmless, but it is worth being aware of. The scheduled cleanup (cleanupOldEvents) also runs on every instance, so at 2 AM you have N instances all running batched deletions simultaneously against the same table. At 3 instances with 100ms sleep between batches this is fine. At 10 instances it adds up. A future consideration is making scheduled maintenance jobs run on only one instance (distributed lock via SELECT FOR UPDATE, or a @ConditionalOnProperty flag, or an external leader-election mechanism like a Redis lock).

Future considerations as scale grows further
Read replica for queries. The /statistics endpoint, gauge metrics, the deduplication existsByUniqueKey check, and findFirstByExternalJobIdOrderByScheduledAtDesc are all reads. At high volume, routing all reads to a read replica frees the primary for writes (poll + persist + update). Spring's @Transactional(readOnly = true) already marks all query methods correctly — adding a AbstractRoutingDataSource that routes read-only transactions to the replica is a surgical change requiring no service-layer modifications. The existsByUniqueKey deduplication check in the Kafka consumer is particularly worth routing to a replica since it fires on every ingested message.

Partition-aware event lookup. findById(UUID) and findFirstByExternalJobIdOrderByScheduledAtDesc have no scheduled_at predicate. PostgreSQL must scan all partitions. As the table grows across many monthly partitions these queries do partition-level sequential scans. Adding scheduled_at to the lookup API (even as an optional hint) allows partition pruning. Alternatively, maintain a small lookup table (event_id → partition_month) for O(1) routing.

Kafka consumer scaling. The ingestion consumer runs with concurrency: 10 (configurable via spring.kafka.listener.concurrency). With 24 partitions and 3 instances you have 30 consumer threads competing for 24 partitions — 6 threads idle. Tune concurrency to partitions / instances to avoid idle threads. At 10M/day the consumer is not the bottleneck (Kafka ingestion is fast), but if events get larger or validation gets heavier it becomes relevant.

Dead letter observability. Currently DLQ messages are sent to Kafka and logged. There is no endpoint to query DLQ contents, no count metric for DLQ accumulation rate, and no alerting threshold surfaced in the UI. The Prometheus alert DeadLetterEventsAccumulating fires if the DB-persisted DEAD_LETTER count grows, but messages that fail before DB persistence (malformed Kafka messages, partition creation failures) go to Kafka DLQ silently. A DLQ consumer that persists failed messages to the DB with an INGESTION_FAILED status would make the full failure picture visible.

Scheduled event pre-warming. At 10M/day the poll loop fetches up to 100 events per second. As long as the batch is always full, throughput is at capacity. But if events are unevenly distributed (e.g. 80% scheduled for 9 AM), the scheduler drains its batch in <1 second and then polls empty for the remaining time. A pre-load window that fetches the next 60 seconds of events into memory would smooth delivery latency for time-critical events and reduce polling round-trips. This is a future optimisation, not a current problem.

Connection pool sizing at higher concurrency. Already mentioned above — as you scale to 3+ instances each doing 100 concurrent deliveries, revisit maximum-pool-size. A good rule of thumb for a write-heavy workload is (cores * 2) + effective_spindle_count, which on modern cloud PostgreSQL (NVMe, 8 cores) suggests ~20 per instance is actually fine for the DB side — the constraint is really the total connections across all instances. With 3 instances at 20 each = 60 connections, PostgreSQL's default max_connections=100 starts to feel tight. Consider PgBouncer in front of the DB if you run more than 4-5 instances.

Style observations
The code is consistent and readable. A few minor things worth noting in a code review:

EventController (/api/v1/test-events) appears to be a leftover stub from an earlier spike — it accepts any JSON body and returns 200 with no implementation. It should either be wired up or deleted before production.
getCleanupStats() in EventCleanupService returns a CleanupStats record but the comment says "This would require a count query - simplified here" — the method returns config values, not actual stats. Either implement it properly or remove it.
AsyncConfig defines eventProcessingExecutor and schedulerExecutor beans that are never injected anywhere (we use Executors.newVirtualThreadPerTaskExecutor() inline in the poll loop). They are harmless but they are dead config — either use them or remove them.
management.endpoints.web.exposure.include: "*" exposes every actuator endpoint including /actuator/env (prints all config including env vars) and /actuator/heapdump. In production this should be locked down to at minimum health,info,prometheus.
Verdict
The architecture is production-ready for 10M events/day on a single well-sized node, and scales horizontally with no coordination changes needed. The three things I would fix before going to production at scale are:

Increase maximum-pool-size to ~50 to avoid connection starvation under concurrent delivery load
Fix the double COUNT(*) gauge queries (remove the live query from the Gauge lambda, keep only MetricsUpdater)
Lock down actuator endpoint exposure
Everything else is either already correct or a future-scale consideration at 50M+ events/day.

