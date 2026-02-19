package io.softwarestrategies.scheduledevent.repository;

import io.softwarestrategies.scheduledevent.domain.EventStatus;
import io.softwarestrategies.scheduledevent.domain.ScheduledEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository for scheduled events with optimized queries for high-throughput processing.
 *
 * Uses PostgreSQL-specific features:
 * - SELECT FOR UPDATE SKIP LOCKED for concurrent processing
 * - Partitioned queries via partition_key
 */
@Repository
public interface ScheduledEventRepository extends JpaRepository<ScheduledEvent, UUID> {

	/**
	 * Find events ready for execution using SELECT FOR UPDATE SKIP LOCKED.
	 * This allows multiple workers to process events concurrently without conflicts.
	 *
	 * @param status        Event status to consider (typically PENDING)
	 * @param scheduledBefore Events scheduled before this time are ready
	 * @param limit         Maximum number of events to fetch
	 * @return List of events ready for processing
	 */
	@Query(value = """
            SELECT * FROM scheduled_events
            WHERE status = :status
            AND scheduled_at <= :scheduledBefore
            AND (lock_expires_at IS NULL OR lock_expires_at < :now)
            ORDER BY scheduled_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
	List<ScheduledEvent> findAndLockEventsForProcessing(
			@Param("status") String status,
			@Param("scheduledBefore") Instant scheduledBefore,
			@Param("now") Instant now,
			@Param("limit") int limit);

	/**
	 * Find event by ID (handles partitioned table where PK is composite)
	 */
	@Query("SELECT e FROM ScheduledEvent e WHERE e.id = :id")
	Optional<ScheduledEvent> findById(@Param("id") UUID id);

	/**
	 * Find events by external job ID
	 */
	Optional<ScheduledEvent> findByExternalJobId(String externalJobId);

	/**
	 * Find all events by external job ID (may have duplicates with different scheduled times)
	 */
	List<ScheduledEvent> findAllByExternalJobId(String externalJobId);

	/**
	 * Find events by source and status
	 */
	List<ScheduledEvent> findBySourceAndStatus(String source, EventStatus status);

	/**
	 * Count events by status
	 */
	long countByStatus(EventStatus status);

	/**
	 * Count events scheduled within a time range
	 */
	@Query("SELECT COUNT(e) FROM ScheduledEvent e WHERE e.scheduledAt BETWEEN :start AND :end")
	long countByScheduledAtBetween(@Param("start") Instant start, @Param("end") Instant end);

	/**
	 * Find events with expired locks (stale locks from crashed workers)
	 */
	@Query("SELECT e FROM ScheduledEvent e WHERE e.status = :status AND e.lockExpiresAt < :now")
	List<ScheduledEvent> findEventsWithExpiredLocks(@Param("status") EventStatus status, @Param("now") Instant now);

	/**
	 * Bulk update to release expired locks
	 */
	@Modifying
	@Query(value = """
            UPDATE scheduled_events 
            SET status = 'PENDING', 
                locked_by = NULL, 
                lock_expires_at = NULL,
                updated_at = :now
            WHERE status = 'PROCESSING' 
            AND lock_expires_at < :now
            """, nativeQuery = true)
	int releaseExpiredLocks(@Param("now") Instant now);

	/**
	 * Delete completed events older than the specified time (for cleanup job)
	 */
	@Modifying
	@Query(value = """
            DELETE FROM scheduled_events 
            WHERE status IN ('COMPLETED', 'DEAD_LETTER', 'CANCELLED')
            AND executed_at < :cutoff
            LIMIT :batchSize
            """, nativeQuery = true)
	int deleteCompletedEventsBefore(@Param("cutoff") Instant cutoff, @Param("batchSize") int batchSize);

	/**
	 * Delete events by partition key range (for partition management)
	 */
	@Modifying
	@Query(value = """
            DELETE FROM scheduled_events 
            WHERE partition_key BETWEEN :startKey AND :endKey
            AND status IN ('COMPLETED', 'DEAD_LETTER', 'CANCELLED')
            """, nativeQuery = true)
	int deleteByPartitionKeyRange(@Param("startKey") int startKey, @Param("endKey") int endKey);

	/**
	 * Find events due in the immediate future (for pre-loading)
	 */
	@Query("""
            SELECT e FROM ScheduledEvent e 
            WHERE e.status = :status 
            AND e.scheduledAt BETWEEN :now AND :until
            ORDER BY e.scheduledAt ASC
            """)
	List<ScheduledEvent> findUpcomingEvents(
			@Param("status") EventStatus status,
			@Param("now") Instant now,
			@Param("until") Instant until);

	/**
	 * Update event status atomically
	 */
	@Modifying
	@Query("""
            UPDATE ScheduledEvent e 
            SET e.status = :newStatus, 
                e.updatedAt = :now,
                e.executedAt = :executedAt,
                e.lockedBy = :lockedBy,
                e.lockExpiresAt = :lockExpiresAt
            WHERE e.id = :id AND e.status = :expectedStatus
            """)
	int updateStatusAtomically(
			@Param("id") UUID id,
			@Param("expectedStatus") EventStatus expectedStatus,
			@Param("newStatus") EventStatus newStatus,
			@Param("now") Instant now,
			@Param("executedAt") Instant executedAt,
			@Param("lockedBy") String lockedBy,
			@Param("lockExpiresAt") Instant lockExpiresAt);

	/**
	 * Cancel an event by external job ID
	 */
	@Modifying
	@Query("""
            UPDATE ScheduledEvent e 
            SET e.status = :cancelledStatus, e.updatedAt = :now 
            WHERE e.externalJobId = :externalJobId AND e.status = :pendingStatus
            """)
	int cancelByExternalJobId(
			@Param("externalJobId") String externalJobId,
			@Param("now") Instant now,
			@Param("cancelledStatus") EventStatus cancelledStatus,
			@Param("pendingStatus") EventStatus pendingStatus);

	/**
	 * Get statistics for monitoring
	 */
	@Query(value = """
            SELECT status, COUNT(*) as count 
            FROM scheduled_events 
            GROUP BY status
            """, nativeQuery = true)
	List<Object[]> getStatusStatistics();

	/**
	 * Check if an event exists by message ID (for idempotency)
	 */
	@Query(value = """
            SELECT EXISTS(
                SELECT 1 FROM scheduled_events 
                WHERE external_job_id = :externalJobId 
                AND source = :source 
                AND scheduled_at = :scheduledAt
            )
            """, nativeQuery = true)
	boolean existsByUniqueKey(
			@Param("externalJobId") String externalJobId,
			@Param("source") String source,
			@Param("scheduledAt") Instant scheduledAt);
}