package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.extractOpaqueEntries;

/**
 * This class represents the entity responsible of writing streams' snapshots into the standby cluster DB.
 *
 * Snapshot sync is the process of transferring a snapshot of the DB, for this reason, data is temporarily applied
 * to shadow streams in an effort to avoid inconsistent states. Once all the data is received, the shadow streams
 * are applied into the actual streams.
 */

@Slf4j
@NotThreadSafe
public class StreamsSnapshotWriter implements SnapshotWriter {

    private static final String SHADOW_STREAM_SUFFIX = "_SHADOW";

    // Mapping from regular stream Id to stream Name
    private final HashMap<UUID, String> streamViewMap;
    private final CorfuRuntime rt;

    private long topologyConfigId;
    private long srcGlobalSnapshot; // The source snapshot timestamp
    private long recvSeq;
    private Optional<SnapshotSyncStartMarker> snapshotSyncStartMarker;
    private final Map<UUID, List<UUID>> dataStreamToTagsMap;
    private final Set<UUID> mergeOnlyStreams;

    @Getter
    private final LogReplicationMetadataManager logReplicationMetadataManager;
    // Mapping from regular stream Id to shadow stream Id
    private final HashMap<UUID, UUID> regularToShadowStreamId;

    // Represents the actual replicated streams from active. This is a subset of all regular streams in
    // regularToShadowStreamId map
    private Set<UUID> replicatedStreamIds = new HashSet<>();

    @Getter
    private Phase phase;

    public StreamsSnapshotWriter(CorfuRuntime rt, LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        this.rt = rt;
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.streamViewMap = new HashMap<>();
        this.regularToShadowStreamId = new HashMap<>();
        this.phase = Phase.TRANSFER_PHASE;
        this.snapshotSyncStartMarker = Optional.empty();
        this.dataStreamToTagsMap = config.getDataStreamToTagsMap();
        this.mergeOnlyStreams = config.getMergeOnlyStreams();

        initializeShadowStreams(config);
    }

    /**
     * Create shadow streams.
     *
     * We create a shadow stream per stream to replicate. A shadow stream aims to accumulate updates
     * temporarily while the (full) snapshot sync completes. Shadow streams aim to avoid inconsistent
     * states while data is still being transferred from active to standby.
     *
     * We currently, wait for snapshot sync to complete before applying data in shadow streams
     * to the actual streams, this means that there is still a window of inconsistency as apply is not atomic,
     * but at least inconsistency is at a point where there is guarantee of all data being available on the receiver.
     * In the future, we will support Table Aliasing which will enable atomic flip from shadow to regular streams, avoiding
     * complete inconsistency.
     */
    private void initializeShadowStreams(LogReplicationConfig config) {
        // For every stream create a shadow stream which name is unique based
        // on the original stream and a suffix.
        for (String streamName : config.getStreamsToReplicate()) {
            String shadowStreamName = streamName + SHADOW_STREAM_SUFFIX;
            UUID streamId = CorfuRuntime.getStreamID(streamName);
            UUID shadowStreamId = CorfuRuntime.getStreamID(shadowStreamName);
            regularToShadowStreamId.put(streamId, shadowStreamId);
            regularToShadowStreamId.put(shadowStreamId, streamId);
            streamViewMap.put(streamId, streamName);

            log.trace("Shadow stream=[{}] for regular stream=[{}] name=({})", shadowStreamId, streamId, streamName);
        }

        log.info("Stream tag map for streaming on Standby/Sink total={}, streams={}", dataStreamToTagsMap.size(),
                dataStreamToTagsMap);
    }

    /**
     * If the metadata has wrong message type or baseSnapshot, throw an exception
     * @param metadata
     */
    private void verifyMetadata(LogReplicationEntryMetadataMsg metadata) {
        if (metadata.getEntryType() != LogReplicationEntryType.SNAPSHOT_MESSAGE ||
                metadata.getSnapshotTimestamp() != srcGlobalSnapshot ||
                metadata.getSnapshotSyncSeqNum() != recvSeq) {
            log.error("Expected snapshot={}, received snapshot={}, expected seq={}, received seq={}",
                    srcGlobalSnapshot, metadata.getSnapshotTimestamp(), metadata.getSnapshotSyncSeqNum(), recvSeq);
            throw new ReplicationWriterException("Snapshot message out of order");
        }
    }

    /**
     * Reset snapshot writer state
     *
     * @param topologyId topology epoch
     * @param snapshot base snapshot timestamp
     */
    public void reset(long topologyId, long snapshot) {
        log.debug("Reset snapshot writer, snapshot={}, topologyConfigId={}", snapshot, topologyId);
        topologyConfigId = topologyId;
        srcGlobalSnapshot = snapshot;
        recvSeq = 0;
        phase = Phase.TRANSFER_PHASE;
        snapshotSyncStartMarker = Optional.empty();
        replicatedStreamIds.clear();
    }

    /**
     * Process updates to shadow stream (temporal stream)
     *
     * @param smrEntries
     * @param currentSeqNum
     * @param shadowStreamUuid
     */
    private void processUpdatesShadowStream(List<SMREntry> smrEntries, Long currentSeqNum, UUID shadowStreamUuid,
                                            UUID snapshotSyncId) {
        CorfuStoreMetadata.Timestamp timestamp;

        try (TxnContext txn = logReplicationMetadataManager.getTxnContext()) {
            updateLog(txn, smrEntries, shadowStreamUuid);
            logReplicationMetadataManager.appendUpdate(txn,
                    LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER, currentSeqNum);
            timestamp = txn.commit();
        }

        if (!snapshotSyncStartMarker.isPresent()) {
            try (TxnContext txn = logReplicationMetadataManager.getTxnContext()) {
                logReplicationMetadataManager.setSnapshotSyncStartMarker(txn, snapshotSyncId, timestamp);
                snapshotSyncStartMarker = Optional.of(new SnapshotSyncStartMarker(snapshotSyncId, timestamp.getSequence()));
                txn.commit();
            }
        }

        log.debug("Process entries total={}, set sequence number {}", smrEntries.size(), currentSeqNum);
    }

    /**
     * Write a list of SMR entries to the specified stream log.
     *
     * @param smrEntries
     * @param streamId
     */
    private void updateLog(TxnContext txnContext, List<SMREntry> smrEntries, UUID streamId) {
        Map<LogReplicationMetadataType, Long> metadataMap = logReplicationMetadataManager.queryMetadata(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
                LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);
        long persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistedSequenceNum = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);

        if (topologyConfigId != persistedTopologyConfigId || srcGlobalSnapshot != persistedSnapshotStart) {
            log.warn("Skip processing opaque entry. Current topologyConfigId={}, srcGlobalSnapshot={}, currentSeqNum={}, " +
                            "persistedTopologyConfigId={}, persistedSnapshotStart={}, persistedLastSequenceNum={}", topologyConfigId,
                    srcGlobalSnapshot, recvSeq, persistedTopologyConfigId, persistedSnapshotStart, persistedSequenceNum);
            return;
        }

        logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
        logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, srcGlobalSnapshot);

        for (SMREntry smrEntry : smrEntries) {
            txnContext.logUpdate(streamId, smrEntry, dataStreamToTagsMap.get(streamId));
        }
    }

    /**
     * Apply updates to shadow stream (temporarily) to avoid data
     * inconsistency until full snapshot has been transferred.
     *
     * @param message snapshot log entry
     */
    @Override
    public void apply(LogReplicationEntryMsg message) {

        verifyMetadata(message.getMetadata());

        if (message.getMetadata().getSnapshotSyncSeqNum() != recvSeq ||
                message.getMetadata().getEntryType() != LogReplicationEntryType.SNAPSHOT_MESSAGE) {
            log.error("Received {} Expecting snapshot message sequencer number {} != recvSeq {} or wrong message type {} expecting {}",
                    message.getMetadata(), message.getMetadata().getSnapshotSyncSeqNum(), recvSeq,
                    message.getMetadata().getEntryType(), LogReplicationEntryType.SNAPSHOT_MESSAGE);
            throw new ReplicationWriterException("Message is out of order or wrong type");
        }

        List<OpaqueEntry> opaqueEntryList = extractOpaqueEntries(message);

        // For snapshot message, it has only one opaque entry.
        if (opaqueEntryList.size() > 1) {
            log.error(" Get {} instead of one opaque entry in Snapshot Message", opaqueEntryList.size());
            return;
        }

        OpaqueEntry opaqueEntry = opaqueEntryList.get(0);
        if (opaqueEntry.getEntries().keySet().size() != 1) {
            log.error("The opaqueEntry has more than one entry {}", opaqueEntry);
            return;
        }
        UUID regularStreamId = opaqueEntry.getEntries().keySet().stream().findFirst().get();


        // When an upgrade has been performed and Active's streams to replicate set has streams not
        // present in Standby's set, they will be dropped for version compatibility consideration
        if (!regularToShadowStreamId.containsKey(regularStreamId)) {
            log.warn("Stream {} sent from Active is not expected in Standby. LR could be undergoing a rolling upgrade", regularStreamId);
            recvSeq++;
            return;
        }

        // Clear regular stream on-demand (i.e., as streams come) and only on the first occurrence
        if (!replicatedStreamIds.contains(regularStreamId)) {
            // Note: we should not clear the shadow stream as this could overwrite our mergeOnlyStreams when
            // shadow stream is applied to the regular stream. Shadow streams are seeked on each replication cycle
            // and are GC'ed by the checkpoint / trim.
            clearRegularStream(regularStreamId);
            replicatedStreamIds.add(regularStreamId);
        }

        processUpdatesShadowStream(opaqueEntry.getEntries().get(regularStreamId), message.getMetadata().getSnapshotSyncSeqNum(),
                regularToShadowStreamId.get(regularStreamId), getUUID(message.getMetadata().getSyncRequestId()));
        recvSeq++;
    }

    /**
     * Clear regular stream and its corresponding shadow stream
     *
     * @param streamId
     */
    private void clearRegularStream(UUID streamId) {
        try {
            boolean mergeOnlyStream = mergeOnlyStreams.contains(streamId);

            if (!mergeOnlyStream) {
                IRetry.build(IntervalRetry.class, () -> {
                    try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
                        logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
                        clearStream(streamId, txnContext);
                        CorfuStoreMetadata.Timestamp ts = txnContext.commit();
                        log.trace("Clear {} committed at :: {}", streamId, ts.getSequence());
                    } catch (TransactionAbortedException tae) {
                        log.error("Error while attempting to clear tables.", tae);
                        throw new RetryNeededException();
                    }
                    return null;
                }).run();
            } else {
                log.debug("Do not clear stream={} (merge stream)", streamId);
            }
        } catch(InterruptedException e){
            log.error("Unrecoverable exception when attempting to clear tables.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private void clearStream(UUID streamId, TxnContext txnContext) {
        SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
        txnContext.logUpdate(streamId, entry, dataStreamToTagsMap.get(streamId));
    }

    @Override
    public void apply(List<LogReplicationEntryMsg> messages) {
        for (LogReplicationEntryMsg msg : messages) {
            apply(msg);
        }
    }

    /**
     * Read from the shadow table and write to the original stream
     *
     * @param streamId regular stream id
     * @param snapshot base snapshot timestamp
     */
    private void applyShadowStream(UUID streamId, long snapshot) {
        log.debug("Apply shadow stream for stream {}, snapshot={}", streamId, snapshot);
        log.debug("Current addresses of stream {} :: {}", streamId,
                rt.getSequencerView().getStreamAddressSpace(new StreamAddressRange(streamId, Long.MAX_VALUE, Address.NON_ADDRESS)));
        UUID shadowStreamId = regularToShadowStreamId.get(streamId);

        // In order to avoid data loss as part of a plugin failing to successfully
        // stop/resume checkpoint and trim. We will not ignore trims on the shadow stream.
        // However, because in between different snapshot cycles, the shadow stream could
        // have been trimmed (without a checkpoint) we will sync the stream up to the first
        // valid known position during the current cycle (i.e. the first applied entry in
        // the current snapshot cycle, which should not have been trimmed by the time of
        // applying the shadow stream).
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        // This variable reflects the minimum timestamp for all shadow streams in the current snapshot cycle.
        // We seek up to this address, assuming that no trim should occur beyond this snapshot start
        long currentMinShadowStreamTimestamp = logReplicationMetadataManager.getMinSnapshotSyncShadowStreamTs();
        OpaqueStream shadowOpaqueStream = new OpaqueStream(rt.getStreamsView().get(shadowStreamId, options));
        shadowOpaqueStream.seek(currentMinShadowStreamTimestamp);
        Stream<OpaqueEntry> shadowStream = shadowOpaqueStream.streamUpTo(snapshot);

        Iterator<OpaqueEntry> iterator = shadowStream.iterator();
        while (iterator.hasNext()) {
            OpaqueEntry opaqueEntry = iterator.next();
            List<SMREntry> smrEntries =  opaqueEntry.getEntries().get(shadowStreamId);

            try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
                updateLog(txnContext, smrEntries, streamId);
                CorfuStoreMetadata.Timestamp ts = txnContext.commit();
                log.debug("Applied shadow stream for stream {} on address :: {}", streamId, ts.getSequence());
            }

            log.debug("Process entries count={}", smrEntries.size());
        }
    }

    /**
     * Read from shadowStream and append/apply to the actual stream
     */
    public void applyShadowStreams() {
        long snapshot = rt.getAddressSpaceView().getLogTail();
        log.debug("Apply Shadow Streams, total={}", streamViewMap.size());
        for (UUID regularStreamId : streamViewMap.keySet()) {
            applyShadowStream(regularStreamId, snapshot);
        }
        // Invalidate client cache after snapshot sync is completed, as shadow streams are no longer useful in the cache
        rt.getAddressSpaceView().invalidateClientCache();
        replicatedStreamIds.clear();
    }

    /**
     * Start Snapshot Sync Apply, i.e., move data from shadow streams to actual streams
     */
    public void startSnapshotSyncApply() {
        phase = Phase.APPLY_PHASE;

        // Get the number of entries to apply
        long seqNum = logReplicationMetadataManager.queryMetadata(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);

        // Only if there is data to be applied
        if (seqNum != Address.NON_ADDRESS) {
            log.debug("Start applying shadow streams, seqNum={}", seqNum);
            applyShadowStreams();
        }
    }

    /**
     * Clear local updated streams for which no data was transferred from active
     * (hence were not cleared on the replication/transfer path)
     *
     * Note: streams could be locally written while this node had no assigned role.
     */
    public void clearLocalStreams() {
        // Iterate over all streams to replicate (as obtained from configuration) and accumulate
        // those for which no data came from active, to make a single call
        // to the sequencer for log tails and discover those with local writes, to be cleared.

        // Note: we cannot clear any stream which has not evidenced updates either on active or standby because
        // we would be enforcing an update without opening the stream, hence, leading to "apparent" data loss as
        // checkpoint won't run on these streams
        Set<UUID> streamsToQuery = streamViewMap.keySet().stream()
                .filter(id -> !replicatedStreamIds.contains(id))
                .collect(Collectors.toCollection(HashSet::new));

        log.debug("Total of {} streams were replicated from active out of {}, sequencer query for {}, streamsToQuery={}",
                replicatedStreamIds.size(), streamViewMap.size(), streamsToQuery.size(), streamsToQuery);
        Set<UUID> streamsToClear = new HashSet<>();
        TokenResponse tokenResponse = rt.getSequencerView().query(streamsToQuery.toArray(new UUID[0]));
        streamsToQuery.forEach(streamId -> {
            if (tokenResponse.getStreamTail(streamId) != Address.NON_EXIST) {
                streamsToClear.add(streamId);
            }
        });

        if (!streamsToClear.isEmpty()) {
            log.debug("Clear streams with local writes, total={}, streams={}", streamsToClear.size(), streamsToClear);
            clearStreams(streamsToClear);
        } else {
            log.debug("No local written streams were found, nothing to clear.");
        }
    }

    private void clearStreams(Set<UUID> streamsToClear) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txnContext = logReplicationMetadataManager.getTxnContext()) {
                    logReplicationMetadataManager.appendUpdate(txnContext, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
                    streamsToClear.forEach(streamId -> {
                        if (!mergeOnlyStreams.contains(streamId)) {
                            clearStream(streamId, txnContext);
                        }
                    });
                    CorfuStoreMetadata.Timestamp ts = txnContext.commit();
                    log.trace("Clear {} streams committed at :: {}", streamsToClear.size(), ts.getSequence());
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to clear locally written streams.", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to clear locally written streams.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    enum Phase {
        TRANSFER_PHASE,
        APPLY_PHASE
    }

    /**
     * This class represents a unique identification of the start of a snapshot cycle.
     * It is represented by the unique identifier of the snapshot sync cycle and
     * the min (first timestamp) to a shadow stream in this snapshot cycle.
     *
     * This is used to validate Trim Exceptions that can lead to data loss.
     */
    static class SnapshotSyncStartMarker {
        private final UUID snapshotId;
        private final long minShadowStreamTimestamp;

        public SnapshotSyncStartMarker(UUID snapshotId, long minShadowStreamTimestamp) {
            this.snapshotId = snapshotId;
            this.minShadowStreamTimestamp = minShadowStreamTimestamp;
        }
    }
}
