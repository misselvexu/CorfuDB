package org.corfudb.infrastructure;

import io.micrometer.core.instrument.Gauge;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.view.Address;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Sequencer server cache.
 * Contains transaction conflict-resolution data structures.
 * <p>
 * The conflictKeyMap maps conflict keys (stream id + key) to versions (long), illustrated below:
 * Conflict Key | ck1 | ck2 | ck3 | ck4
 * Version | v1 | v1 | v2 | v3
 * Consider the case where we need to insert a new conflict key (ck), but the cache is full,
 * we need to evict the oldest conflict keys. In the example above, we can't just evict ck1,
 * we need to evict all keys that map to v1, so we need to evict ck1 and ck2,
 * this eviction policy is FIFO on the version number. The simple FIFO approach in Caffeine doesn't work here,
 * as it may evict ck1, but not ck2. Notice that we also can't evict ck3 before the keys for v1,
 * that's because it will create holes in the resolution window and can lead to incorrect resolutions.
 * <p>
 * We use TreeMap as a sliding window on the versions, where a version can map to multiple keys,
 * so we also need to maintain the beginning of the window which is the maxConflictWildcard variable.
 * <p>
 * SequencerServerCache achieves consistency by using single threaded cache. It's done by following code:
 * `.executor(Runnable::run)`
 */
@NotThreadSafe
@Slf4j
public class SequencerServerCache {
    /**
     * TX conflict-resolution information:
     * a cache of recent conflict keys and their latest global-log position.
     */

    // As the sequencer cache is used by a single thread, it is safe to use HashMap/LinkedHashSet/TreeMap
    private final Map<ConflictTxStream, Long> conflictKeyMap;
    private final SortedMap<Long, Set<ConflictTxStream>> versionMap = new TreeMap<>();

    private Long firstAddress = Address.NOT_FOUND;

    @Getter
    private final int cacheSize; // The max number of entries in SequencerServerCache

    /**
     * A "wildcard" representing the maximal update timestamp of
     * all the conflict keys which were evicted from the cache
     */
    @Getter
    private long maxConflictWildcard;

    /**
     * maxConflictNewSequencer represents the max update timestamp of all the conflict keys
     * which were evicted from the cache by the time this server is elected
     * the primary sequencer. This means that any snapshot timestamp below this
     * actual threshold would abort due to NEW_SEQUENCER cause.
     */
    @Getter
    private final long maxConflictNewSequencer;

    @Getter
    private final String conflictKeysCounterName = "sequencer.conflict-keys.size";
    @Getter
    private final String windowSizeName = "sequencer.cache.window";

    /**
     * The cache limited by size.
     * For a synchronous cache we are using a same-thread executor (Runnable::run)
     * https://github.com/ben-manes/caffeine/issues/90
     *
     * @param cacheSize cache size
     */
    public SequencerServerCache(int cacheSize, long maxConflictNewSequencer) {
        this.cacheSize = cacheSize;

        maxConflictWildcard = maxConflictNewSequencer;
        this.maxConflictNewSequencer = maxConflictNewSequencer;
        conflictKeyMap = MeterRegistryProvider
                .getInstance()
                .map(registry ->
                        registry.gauge(conflictKeysCounterName, Collections.emptyList(),
                                new HashMap<ConflictTxStream, Long>(), Map::size))
                .orElse(new HashMap<>());
        MeterRegistryProvider.getInstance().map(registry ->
                Gauge.builder(windowSizeName,
                        conflictKeyMap, Map::size).register(registry));
    }

    /**
     * Returns the value associated with the {@code key} in this cache,
     * or {@code Address.NON_ADDRESS} if there is no cached value for the {@code key}.
     *
     * @param conflictKey conflict stream
     * @return global address
     */
    public Long get(ConflictTxStream conflictKey) {
        return conflictKeyMap.getOrDefault(conflictKey, Address.NON_ADDRESS);
    }

    /**
     * The first (smallest) address in the cache.
     */
    public long firstAddress() {
        return firstAddress;
    }

  /**
   * Invalidate the records with the firstAddress. It could be one or multiple records
   *
   * @return the number of entries has been invalidated and removed from the cache.
   */
  private int invalidateSmallestTxVersion() {
      if (versionMap.isEmpty()) {
          return 0;
      }

      final Set<ConflictTxStream> entriesToDelete = versionMap.remove(firstAddress);
      final int numDeletedEntries = entriesToDelete.size();
      log.trace("invalidateSmallestTxVersion: items evicted {} min address {}", numDeletedEntries, firstAddress);
      entriesToDelete.forEach(conflictKeyMap::remove);
      maxConflictWildcard = Math.max(maxConflictWildcard, firstAddress);

      if (versionMap.isEmpty()) {
          firstAddress = Address.NOT_FOUND;
      } else {
          firstAddress = versionMap.firstKey();
      }

      return numDeletedEntries;
  }

    /**
     * Invalidate all records up to a trim mark (not included).
     *
     * @param trimMark trim mark
     */
    public void invalidateUpTo(long trimMark) {
        log.debug("Invalidate sequencer cache. Trim mark: {}", trimMark);
        int entries = 0;
        int pqEntries = 0;
        while (Address.isAddress(firstAddress) && firstAddress < trimMark) {
            pqEntries += invalidateSmallestTxVersion();
            entries++;
        }
        final int numPqEntries = pqEntries;
        MicroMeterUtils.measure(numPqEntries, "sequencer.cache.evictions");
        log.info("Invalidated entries {} addresses {}", pqEntries, entries);
    }

    /**
     * The cache size as the number of entries
     *
     * @return cache size
     */
    public int size() {
        return conflictKeyMap.size();
    }

    /**
     * Put a value in the cache
     *
     * @param conflictStream conflict stream
     */
    public boolean put(ConflictTxStream conflictStream) {
        Long val = conflictKeyMap.getOrDefault(conflictStream, Address.NON_ADDRESS);
        if (val > conflictStream.txVersion) {
            log.error("For key {} the new entry address {} is smaller than the entry " +
                    "address {} in cache. There is a sequencer regression.",
                    conflictStream, conflictStream.txVersion, val);
            return false;
        } else if (val == conflictStream.txVersion) {
            return true; // No need to update the cache
        }

        Set<ConflictTxStream> entries;

        // Remove the entry if conflict key was present with an older version
        if (val != Address.NON_ADDRESS) {
            entries = versionMap.get(val);
            entries.remove(conflictStream);
            if (entries.isEmpty()) {
                versionMap.remove(val);
            }
        }

        entries = versionMap.getOrDefault(conflictStream.txVersion, new LinkedHashSet<>());
        entries.add(conflictStream);
        versionMap.putIfAbsent(conflictStream.txVersion, entries);
        conflictKeyMap.put(conflictStream, conflictStream.txVersion);
        firstAddress = versionMap.firstKey();

        while (conflictKeyMap.size() > cacheSize) {
            invalidateSmallestTxVersion();
        }

        return true;
    }

    /**
     * Contains the conflict hash code for a stream ID and conflict param.
     */
    @EqualsAndHashCode
    public static class ConflictTxStream {

        @Getter
        private final UUID streamId;
        @Getter
        private final byte[] conflictParam;

        @EqualsAndHashCode.Exclude
        public final long txVersion;

        public ConflictTxStream(UUID streamId, byte[] conflictParam, long address) {
            this.streamId = streamId;
            this.conflictParam = conflictParam;
            txVersion = address;
        }

        @Override
        public String toString() {
            return streamId.toString() + Arrays.toString(conflictParam);
        }
    }
}
