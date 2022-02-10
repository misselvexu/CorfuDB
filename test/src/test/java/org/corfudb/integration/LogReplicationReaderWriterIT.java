package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.StreamsSnapshotWriter;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.corfudb.utils.LogReplicationStreams;
import org.corfudb.utils.LogReplicationStreams.TableInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class LogReplicationReaderWriterIT extends AbstractIT {
    private static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String WRITER_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;
    private static final int START_VAL = 11;
    private static final int NUM_KEYS = 10;
    private static final int NUM_STREAMS = 2;
    public static final int NUM_TRANSACTIONS = 20;
    public static final String PRIMARY_SITE_ID = "Cluster-Paris";
    public static final int BATCH_SIZE = 2;

    // Enforce to read each entry for each message
    // each log entry size is 62, there are 2 log entry per dataMsg
    // each snapshot entry is 33, there are 4 snapshot entry per dataMsg
    public static final int MAX_MSG_SIZE = 160;

    private static Semaphore waitSem = new Semaphore(1);

    private static final UUID snapshotSyncId = UUID.randomUUID();

    // Connect with server1 to generate data
    private CorfuRuntime srcDataRuntime = null;

    // Connect with server1 to read snapshot data
    private CorfuRuntime readerRuntime = null;

    // Connect with server2 to write snapshot data
    private CorfuRuntime writerRuntime = null;

    // Connect with server2 to verify data
    private CorfuRuntime dstDataRuntime = null;

    private final HashMap<String, CorfuTable<Long, Long>> srcTables = new HashMap<>();
    private final HashMap<String, CorfuTable<Long, Long>> dstTables = new HashMap<>();
    private final HashMap<String, CorfuTable<Long, Long>> shadowTables = new HashMap<>();

    private CorfuRuntime srcTestRuntime;

    private CorfuRuntime dstTestRuntime;

    // Corfu tables in-memory view, used for verification.
    private final HashMap<String, HashMap<Long, Long>> srcHashMap = new HashMap<>();

    // Store messages generated by stream snapshot reader and will play it at the writer side.
    private final List<LogReplicationEntryMsg> msgQ = new ArrayList<>();

    private void setupEnv() throws IOException {
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params);
        srcDataRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcDataRuntime.connect();

        srcTestRuntime = CorfuRuntime.fromParameters(params);
        srcTestRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcTestRuntime.connect();

        readerRuntime = CorfuRuntime.fromParameters(params);
        readerRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        readerRuntime.connect();

        writerRuntime = CorfuRuntime.fromParameters(params);
        writerRuntime.parseConfigurationString(WRITER_ENDPOINT);
        writerRuntime.connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstDataRuntime.connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstTestRuntime.connect();
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt) {
        openStreams(tables, rt, NUM_STREAMS, Serializers.PRIMITIVE, false);
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int num_streams) {
        openStreams(tables, rt, num_streams, Serializers.PRIMITIVE);
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int num_streams,
                                   ISerializer serializer, boolean shadow) {
        for (int i = 0; i < num_streams; i++) {
            String name = "test" + i;
            if (shadow) {
                name = name + "_shadow";
            }
            CorfuTable<Long, Long> table = rt.getObjectsView()
                    .build()
                    .setStreamName(name)
                    .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(serializer)
                    .open();
            tables.put(name, table);
        }
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int num_streams,
                                                 ISerializer serializer) {
        openStreams(tables, rt, num_streams, serializer, false);
    }

    public static void generateData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numKeys, CorfuRuntime rt, long startVal) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : tables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = i + startVal;
                rt.getObjectsView().TXBegin();
                tables.get(name).put(key, key);
                rt.getObjectsView().TXEnd();
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                hashMap.get(name).put(key, key);
            }
        }
    }

    // Generate data with transactions and the same time push the data to the hashtable
    public static void generateTransactions(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numT, CorfuRuntime rt, long startVal) {
        int j = 0;
        for (int i = 0; i < numT; i++) {
            rt.getObjectsView().TXBegin();
            for (String name : tables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = j + startVal;
                tables.get(name).put(key, key);
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                hashMap.get(name).put(key, key);
                j++;
            }
            rt.getObjectsView().TXEnd();
        }
        log.debug("Generate transactions num " + numT);
    }

    public static void verifyData(String tag, HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap) {
        log.debug("\n" + tag);
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);
            log.debug("table " + name + " key size " + table.keySet().size() +
                    " hashMap size " + mapKeys.size());

            assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
            assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
            assertThat(table.keySet().size()).isEqualTo(mapKeys.keySet().size());

            for (Long key : mapKeys.keySet()) {
                assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
            }
        }
    }

    public static void verifyTable(String tag, HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, CorfuTable<Long, Long>> hashMap) {
        log.debug("\n" + tag);
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            CorfuTable<Long, Long> mapKeys = hashMap.get(name);
            log.debug("table " + name + " key size " + table.keySet().size() +
                    " hashMap size " + mapKeys.size());

            assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
            assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
            assertThat(table.keySet().size()).isEqualTo(mapKeys.keySet().size());

            for (Long key : mapKeys.keySet()) {
                assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
            }
        }
    }

    void verifyTxStream(CorfuRuntime rt) {
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        IStreamView txStream = rt.getStreamsView().getUnsafe(ObjectsView.getLogReplicatorStreamId(), options);
        List<ILogData> dataList = txStream.remaining();
        log.debug("\ndataList size " + dataList.size());
        for (ILogData data : txStream.remaining()) {
            log.debug("{}", data);
        }
    }

    public static void printTails(String tag, CorfuRuntime rt0, CorfuRuntime rt1) {
        log.debug("\n" + tag);
        log.debug("src dataTail " + rt0.getAddressSpaceView().getLogTail());
        log.debug("dst dataTail " + rt1.getAddressSpaceView().getLogTail());

    }

    public static void readSnapLogMsgs(List<LogReplicationEntryMsg> msgQ, Set<String> streams, CorfuRuntime rt) {
        readSnapLogMsgs(msgQ, streams, rt, false);
    }

    public static void readSnapLogMsgs(List<LogReplicationEntryMsg> msgQ, Set<String> streams, CorfuRuntime rt, boolean blockOnSem)  {
        int cnt = 0;
        LogReplicationConfig config = new LogReplicationConfig(convertStreamNameToInfo(streams), BATCH_SIZE, MAX_MSG_SIZE);
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            cnt++;

            SnapshotReadMessage snapshotReadMessage = reader.read(snapshotSyncId);
            for (LogReplicationEntryMsg data : snapshotReadMessage.getMessages()) {
                msgQ.add(data);
                log.debug("generate msg " + cnt);
            }

            if (snapshotReadMessage.isEndRead()) {
                break;
            }

            if  (blockOnSem) {
                try {
                    waitSem.acquire();
                } catch (InterruptedException e) {
                    log.info("Caught an interrupted exception ", e);
                }
                blockOnSem = false;
            }
        }
    }

    public static void writeSnapLogMsgs(List<LogReplicationEntryMsg> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(convertStreamNameToInfo(streams), BATCH_SIZE, MAX_MSG_SIZE);
        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(rt, 0, PRIMARY_SITE_ID);
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config, logReplicationMetadataManager);

        if (msgQ.isEmpty()) {
            log.debug("msgQ is empty");
        }

        long topologyConfigId = msgQ.get(0).getMetadata().getTopologyConfigID();
        long snapshot = msgQ.get(0).getMetadata().getSnapshotTimestamp();
        logReplicationMetadataManager.setBaseSnapshotStart(topologyConfigId, snapshot);
        writer.reset(topologyConfigId, snapshot);

        for (LogReplicationEntryMsg msg : msgQ) {
            writer.apply(msg);
        }

        writer.applyShadowStreams();
    }

    public static void readLogEntryMsgs(List<LogReplicationEntryMsg> msgQ, Set<String> streams, CorfuRuntime rt) throws TrimmedException {
        readLogEntryMsgs(msgQ, streams, rt, false);
    }

    public static void readLogEntryMsgs(List<LogReplicationEntryMsg> msgQ, Set<String> streams, CorfuRuntime rt, boolean blockOnce) throws
            TrimmedException {
        LogReplicationConfig config = new LogReplicationConfig(convertStreamNameToInfo(streams), BATCH_SIZE, MAX_MSG_SIZE);
        StreamsLogEntryReader reader = new StreamsLogEntryReader(rt, config);
        reader.setGlobalBaseSnapshot(Address.NON_ADDRESS, Address.NON_ADDRESS);

        LogReplicationEntryMsg entry;

        do {
            entry = reader.read(UUID.randomUUID());

            if (entry != null) {
                msgQ.add(entry);
            }

            if (blockOnce) {
                try {
                    waitSem.acquire();
                } catch (InterruptedException e) {
                    log.info("Caught an InterruptedException ", e);
                }
                blockOnce = false;
            }

            log.debug(" msgQ size " + msgQ.size());

        } while (entry != null);

        assertThat(reader.getLastOpaqueEntry()).isNull();
    }

    public static void writeLogEntryMsgs(List<LogReplicationEntryMsg> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(convertStreamNameToInfo(streams));
        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(rt, 0, PRIMARY_SITE_ID);
        LogEntryWriter writer = new LogEntryWriter(config, logReplicationMetadataManager);

        if (msgQ.isEmpty()) {
            log.debug("msgQ is EMPTY");
        }

        for (LogReplicationEntryMsg msg : msgQ) {
            writer.apply(msg);
        }
    }

    private void accessTxStream(Iterator<ILogData> iterator, int num) {
        int i = 0;
        while (iterator.hasNext() && i++ < num) {
            iterator.next();
        }
    }

    public static void trimAlone(long address, CorfuRuntime rt) {
        // Trim the log
        Token token = new Token(0, address);
        rt.getAddressSpaceView().prefixTrim(token);
        rt.getAddressSpaceView().gc();
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();
        waitSem.release();
        log.debug("\ntrim at " + token + " currentTail " + rt.getAddressSpaceView().getLogTail());
    }

    /**
     * enforce checkpoint entries at the streams.
     */
    public static Token ckStreamsAndTrim(CorfuRuntime rt, HashMap<String, CorfuTable<Long, Long>> tables) {
        MultiCheckpointWriter<CorfuTable<Long, Long>> mcw1 = new MultiCheckpointWriter<>();
        for (CorfuTable<Long, Long> map : tables.values()) {
            mcw1.addMap(map);
        }

        Token checkpointAddress = mcw1.appendCheckpoints(rt, "test");

        // Trim the log
        trimAlone(checkpointAddress.getSequence(), rt);
        return checkpointAddress;
    }

    public void checkpointAndTrim(CorfuRuntime rt) {
        Token token = ckStreamsAndTrim(rt, srcTables);

        Token trimMark = rt.getAddressSpaceView().getTrimMark();

        while (trimMark.getSequence() != (token.getSequence() + 1)) {
            log.debug("trimMark " + trimMark + " trimToken " + token);
            trimMark = rt.getAddressSpaceView().getTrimMark();
        }

        rt.getAddressSpaceView().invalidateServerCaches();
        log.debug("trim " + token);
    }

    private void trimDelay() {
        try {
            while(msgQ.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(1);
            }
            checkpointAndTrim( srcDataRuntime);
        } catch (Exception e) {
            log.debug("caught an exception " + e);
        }
    }

    private void trimAloneDelay() {
        try {
            while(msgQ.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(1);
            }
            trimAlone(srcDataRuntime.getAddressSpaceView().getLogTail(), srcDataRuntime);
        } catch (Exception e) {
            log.debug("caught an exception " + e);
        }
    }

    private static Set<TableInfo> convertStreamNameToInfo(Set<String> streams) {
        Set<TableInfo> infoSet = new HashSet<>();

        for (String streamName : streams) {
            TableInfo info = TableInfo.newBuilder()
                    .setName(streamName)
                    .build();
            infoSet.add(info);
        }

        return infoSet;
    }

    /**
     * Generate some transactions, and start a txstream. Do a trim
     * To see if a trimmed exception happens
     */
    @Test
    public void testTrimmedExceptionForTxStream() throws IOException {
        setupEnv();
        openStreams(srcTables, srcDataRuntime);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_KEYS);

        // Open the log replication stream
        IStreamView txStream = srcTestRuntime.getStreamsView().get(ObjectsView.getLogReplicatorStreamId());
        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();
        Stream<ILogData> stream = txStream.streamUpTo(tail);
        Iterator<ILogData> iterator = stream.iterator();

        Exception result = null;
        checkpointAndTrim(srcDataRuntime);

        try {
            accessTxStream(iterator, (int)tail);
        } catch (Exception e) {
            result = e;
            log.debug("caught an exception " + e + " tail " + tail);
        } finally {
            assertThat(result).isInstanceOf(TrimmedException.class);
        }
    }

    @Test
    public void testOpenTableAfterTrimWithoutCheckpoint () throws IOException {
        final int offset = 20;
        setupEnv();
        openStreams(srcTables, srcDataRuntime);
        generateTransactions(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        trimAlone(srcDataRuntime.getAddressSpaceView().getLogTail() - offset, srcDataRuntime);

        try {
            CorfuTable<Long, Long> testTable = srcTestRuntime.getObjectsView()
                    .build()
                    .setStreamName("test0")
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            testTable.size();
        } catch (Exception e) {
            log.debug("caught a exception " + e);
            assertThat(e).isInstanceOf(TrimmedException.class);
        }
    }

    @Test
    public void testTrimmedExceptionForLogEntryReader() throws Exception {
        setupEnv();

        waitSem = new Semaphore(1);
        waitSem.acquire();

        openStreams(srcTables, srcDataRuntime);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_KEYS);
        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.submit(this::trimAloneDelay);
        Exception result = null;

        try {
            readLogEntryMsgs(msgQ, srcHashMap.keySet(), readerRuntime, true);
        } catch (Exception e) {
            result = e;
            log.debug("msgQ size " + msgQ.size());
            log.debug("caught an exception " + e + " tail " + tail);
        } finally {
            assertThat(result).isInstanceOf(TrimmedException.class);
        }

        tearDownEnv();
        cleanUp();
    }

    private void tearDownEnv() {
        if (srcDataRuntime != null) {
            srcDataRuntime.shutdown();
            srcTestRuntime.shutdown();
            readerRuntime.shutdown();
            writerRuntime.shutdown();
            dstDataRuntime.shutdown();
            dstTestRuntime.shutdown();
        }
    }

    @Test
    public void testTrimmedExceptionForSnapshotReader() throws IOException, InterruptedException {
        setupEnv();
        waitSem = new Semaphore(1);
        waitSem.acquire();
        openStreams(srcTables, srcDataRuntime, 1);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_KEYS);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.submit(this::trimDelay);

        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();
        Exception result = null;

        try {
            readSnapLogMsgs(msgQ, srcHashMap.keySet(), readerRuntime, true);
        } catch (Exception e) {
            result = e;
            log.debug("caught an exception " + e + " tail " + tail);
        } finally {
            log.debug("msgQ size " + msgQ.size());
            assertThat(result).isInstanceOf(TrimmedException.class);
        }
    }


    /**
     * Write to a corfu table and read SMRntries with streamview,
     * redirect the SMRentries to the second corfu server, and verify
     * the second corfu server contains the correct <key, value> pairs
     * @throws Exception
     */
    @Test
    public void testWriteSMREntries() throws Exception {
        // setup environment
        log.debug("\ntest start");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);
        verifyData("after writing to server1", srcTables, srcHashMap);
        printTails("after writing to server1", srcDataRuntime, dstDataRuntime);

        //read streams as SMR entries
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        for (String name : srcHashMap.keySet()) {
            IStreamView srcSV = srcTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID(name), options);
            IStreamView dstSV = dstTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID(name), options);

            List<ILogData> dataList = srcSV.remaining();
            for (ILogData data : dataList) {
                OpaqueEntry opaqueEntry = OpaqueEntry.unpack(data);
                for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                    for (SMREntry entry : opaqueEntry.getEntries().get(uuid)) {
                        dstSV.append(entry);
                    }
                }
            }
        }

        printTails("after writing to dst", srcDataRuntime, dstDataRuntime);
        openStreams(dstTables, writerRuntime);
        verifyData("after writing to dst", dstTables, srcHashMap);
    }

    @Test
    public void testSnapshotTransfer() throws Exception {
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, START_VAL);
        verifyData("after writing to src", srcTables, srcHashMap);

        // generate dump data at dst
        openStreams(dstTables, dstDataRuntime);

        // read snapshot from srcServer and put msgs into Queue
        readSnapLogMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        // play messages at dst server
        writeSnapLogMsgs(msgQ, srcHashMap.keySet(), writerRuntime);

        printTails("after writing to server2", srcDataRuntime, dstDataRuntime);

        // Verify data with hashtable
        verifyTable("after snap write at dst", dstTables, srcTables);
    }

    @Test
    public void testLogEntryTransferWithNoSerializer() throws IOException {
        setupEnv();
        ISerializer serializer = new TestSerializer(Byte.MAX_VALUE);

        openStreams(srcTables, srcDataRuntime, NUM_STREAMS, serializer);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_TRANSACTIONS);

        HashMap<String, CorfuTable<Long, Long>> singleTables = new HashMap<>();
        singleTables.putIfAbsent("test0", srcTables.get("test0"));
        generateTransactions(singleTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_TRANSACTIONS);

        verifyData("after writing to src", srcTables, srcHashMap);

        printTails("after writing to server1", srcDataRuntime, dstDataRuntime);

        verifyTxStream(srcTestRuntime);

        //read snapshot from srcServer and put msgs into Queue
        readLogEntryMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        //play messages at dst server
        writeLogEntryMsgs(msgQ, srcHashMap.keySet(), writerRuntime);

        printTails("after writing to server2", srcDataRuntime, dstDataRuntime);

        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime, NUM_STREAMS, serializer);

        Exception result = null;
        try {
            verifyData("after log writing at dst", dstTables, srcHashMap);
        } catch (Exception e) {
            log.debug("caught an exception");
            result = e;
        } finally {
            assertThat(result).isInstanceOf(SerializerException.class);
        }
    }

    @Test
    public void testLogEntryTransferWithSerializer() throws Exception {
        setupEnv();
        ISerializer serializer = new TestSerializer(Byte.MAX_VALUE);

        openStreams(srcTables, srcDataRuntime, NUM_STREAMS, serializer);
        openStreams(shadowTables, dstDataRuntime, NUM_STREAMS, serializer, true);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_TRANSACTIONS);

        //read snapshot from srcServer and put msgs into Queue
        readLogEntryMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        //play messages at dst server
        writeLogEntryMsgs(msgQ, srcHashMap.keySet(), writerRuntime);

        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime, NUM_STREAMS, serializer);

        dstDataRuntime.getSerializers().registerSerializer(serializer);
        verifyData("after log writing at dst", dstTables, srcHashMap);

        cleanUp();
    }
    
    /**
     * This test verifies that the Log Entry Reader sets the last processed entry
     * as NULL whenever all entries written to the TX stream are of no interest for
     * the replication process (streams present are not intended for replication)
     *
     * If the lastProcessedEntry is not NULL, this ensures that LogEntryReader will not loop
     * forever on the last observed entry.
     *
     * @throws Exception
     */
    @Test
    public void testLogEntryReaderWhenTxStreamNoStreamsToReplicate() throws Exception {
        setupEnv();

        // Declare a set of streams to replicate that do no exist in the source, so the TX stream
        // has streams that are not of interest.
        Set<String> streamsToReplicate = new HashSet<>();
        streamsToReplicate.add("nonExistingStream1");
        streamsToReplicate.add("nonExistingStream2");

        openStreams(srcTables, srcDataRuntime);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, START_VAL);
        verifyData("after writing to src", srcTables, srcHashMap);

        // Confirm we get out of Log Entry Reader when there are streams of no interest in the Tx Stream
        readLogEntryMsgs(msgQ, streamsToReplicate, readerRuntime);

        cleanUp();
    }
}

