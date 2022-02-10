package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.Data;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.utils.LogReplicationStreams.TableInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This class represents any Log Replication Configuration,
 * i.e., set of parameters common across all Clusters.
 */
@Slf4j
@Data
@ToString
public class LogReplicationConfig {

    // Log Replication message timeout time in milliseconds
    public static final int DEFAULT_TIMEOUT_MS = 5000;

    // Log Replication default max number of messages generated at the active cluster for each batch
    public static final int DEFAULT_MAX_NUM_MSG_PER_BATCH = 10;

    // Log Replication default max data message size is 64MB
    public static final int MAX_DATA_MSG_SIZE_SUPPORTED = (64 << 20);

    // Log Replication default max cache number of entries
    // Note: if we want to improve performance for large scale this value should be tuned as it
    // used in snapshot sync to quickly access shadow stream entries, written locally.
    // This value is exposed as a configuration parameter for LR.
    public static final int MAX_CACHE_NUM_ENTRIES = 200;

    // Percentage of log data per log replication message
    public static final int DATA_FRACTION_PER_MSG = 90;

    // Info for all streams to be replicated across sites.
    private final StreamInfo streamsInfo;

    // Streaming tags on Sink/Standby (map data stream id to list of tags associated to it)
    private Map<UUID, List<UUID>> dataStreamToTagsMap = new HashMap<>();

    // Set of streams that shouldn't be cleared on snapshot apply phase, as these
    // streams should be the result of "merging" the replicated data (from active) + local data (on standby).
    // For instance, RegistryTable (to avoid losing local opened tables on standby)
    private Set<UUID> mergeOnlyStreams = new HashSet<>();

    // Snapshot Sync Batch Size(number of messages)
    private int maxNumMsgPerBatch;

    // Max Size of Log Replication Data Message
    private int maxMsgSize;

    // Max Cache number of entries
    private int maxCacheSize;

    /**
     * The max size of data payload for the log replication message.
     */
    private int maxDataSizePerMsg;

    /**
     * Constructor
     *
     * @param streamsToReplicate Info for all streams to be replicated across sites.
     */
    @VisibleForTesting
    public LogReplicationConfig(Set<TableInfo> streamsToReplicate) {
        this(streamsToReplicate, DEFAULT_MAX_NUM_MSG_PER_BATCH, MAX_DATA_MSG_SIZE_SUPPORTED,
                MAX_CACHE_NUM_ENTRIES, null);
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Info for all streams to be replicated across sites.
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<TableInfo> streamsToReplicate, int maxNumMsgPerBatch, int maxMsgSize,
                                int cacheSize, LogReplicationStreamNameTableManager streamInfoManager) {
        this.streamsInfo = new StreamInfo(streamsToReplicate, streamInfoManager);
        this.maxNumMsgPerBatch = maxNumMsgPerBatch;
        this.maxMsgSize = maxMsgSize;
        this.maxCacheSize = cacheSize;
        this.maxDataSizePerMsg = maxMsgSize * DATA_FRACTION_PER_MSG / 100;
    }

    /**
     * Constructor
     *
     * @param streamsToReplicate Info for all streams to be replicated across sites
     * @param maxNumMsgPerBatch snapshot sync batch size (number of entries per batch)
     */
    public LogReplicationConfig(Set<TableInfo> streamsToReplicate, int maxNumMsgPerBatch, int maxMsgSize) {
        this(streamsToReplicate, maxNumMsgPerBatch, maxMsgSize, MAX_CACHE_NUM_ENTRIES, null);
    }

    public LogReplicationConfig(Set<TableInfo> streamsToReplicate, Map<UUID, List<UUID>> streamingTagsMap,
                                Set<UUID> mergeOnlyStreams, int maxNumMsgPerBatch, int maxMsgSize,
                                int cacheSize, LogReplicationStreamNameTableManager streamInfoManager) {
        this(streamsToReplicate, maxNumMsgPerBatch, maxMsgSize, cacheSize, streamInfoManager);
        this.dataStreamToTagsMap = streamingTagsMap;
        this.mergeOnlyStreams = mergeOnlyStreams;
    }

    /**
     * This class represents Log Replication streams info
     */
    public static class StreamInfo {
        private final Set<UUID> streamIds;
        private final LogReplicationStreamNameTableManager streamInfoManager;

        public StreamInfo(@NonNull Set<TableInfo> streamsToReplicate,
                          @NonNull LogReplicationStreamNameTableManager streamInfoManager) {
            this.streamInfoManager = streamInfoManager;
            this.streamIds = new HashSet<>();
            refreshStreamIds(streamsToReplicate, true);
        }

        private void refreshStreamIds(Set<TableInfo> streamsToReplicate, boolean shouldClear) {
            if (shouldClear) {
                this.streamIds.clear();
            }

            for (TableInfo info : streamsToReplicate) {
                if (info.hasField(TableInfo.getDescriptor().findFieldByName("name"))) {
                    this.streamIds.add(CorfuRuntime.getStreamID(info.getName()));
                } else if (info.hasField(TableInfo.getDescriptor().findFieldByName("id"))) {
                    this.streamIds.add(UUID.fromString(info.getId()));
                }
            }
        }

        public void syncWithInfoTable() {
            if (streamInfoManager == null) {
                log.warn("streamInfoManager is null! skipping sync!");
                return;
            }

            Set<TableInfo> fetched = this.streamInfoManager.getStreamsToReplicate();
            // We should refresh the stream ids here because this method is called before snapshot
            // sync and under leadership acquire
            refreshStreamIds(fetched, true);
        }

        public void syncWithTableRegistry(long timestamp) {
            if (streamInfoManager == null) {
                log.warn("streamInfoManager is null! skipping sync!");
                return;
            }

            Set<TableInfo> registryInfo = streamInfoManager.readStreamsToReplicatedFromRegistry(timestamp);
            // We don't refresh the stream ids here because this method is called only before snapshot
            // sync as a supplementary
            refreshStreamIds(registryInfo, false);
        }

        public Set<UUID> getStreamIds() {
            return ImmutableSet.copyOf(streamIds);
        }

        public void addStreams(Set<UUID> streamIdSet) {
            streamInfoManager.addStreamsToInfoTable(streamIdSet);
            Set<TableInfo> fetched = this.streamInfoManager.getStreamsToReplicate();
            refreshStreamIds(fetched, false);
            log.debug("Added new streams {} to streamIds in config", streamIdSet);
        }
    }
}
