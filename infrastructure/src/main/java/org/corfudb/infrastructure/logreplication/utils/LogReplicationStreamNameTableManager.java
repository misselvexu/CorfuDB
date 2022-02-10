package org.corfudb.infrastructure.logreplication.utils;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams.VersionString;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.TableInfo;
import org.corfudb.utils.LogReplicationStreams.Namespace;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Handle creation and maintenance of the Corfu table/s containing names of tables
 * to be replicated.
 * @author pankti-m
 */
@Slf4j
public class LogReplicationStreamNameTableManager {

    public static final String LOG_REPLICATION_STREAMS_INFO_TABLE = "LogReplicationStreams";
    public static final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    private ILogReplicationConfigAdapter logReplicationConfigAdapter;

    private final String pluginConfigFilePath;

    private final CorfuStore corfuStore;

    private final CorfuRuntime runtime;

    private static final String EMPTY_STR = "";

    private static final CommonTypes.Uuid defaultMetadata =
        CommonTypes.Uuid.newBuilder().setLsb(0).setMsb(0).build();

    private static final Set<UUID> MERGE_ONLY_STREAM_ID_LIST = new HashSet<>(Arrays.asList(
            CorfuRuntime.getStreamID(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                    TableRegistry.REGISTRY_TABLE_NAME)),
            CorfuRuntime.getStreamID(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE,
                    TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME))
    ));

    public LogReplicationStreamNameTableManager(CorfuRuntime runtime, String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
        this.corfuStore = new CorfuStore(runtime);
        this.runtime = runtime;

        initStreamNameFetcherPlugin();
    }

    /**
     * This method is for ACTIVE to fetch streams to replicate from LOG_REPLICATION_STREAMS_INFO_TABLE.
     * If it doesn't exist, we will create info table by reading from registry table.
     *
     * @return Set of TableInfo for streams to replicate from ACTIVE
     */
    public Set<TableInfo> getStreamsToReplicate() {
        // Initialize the streamsToReplicate
        if (verifyTableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE) &&
            verifyTableExists(LOG_REPLICATION_STREAMS_INFO_TABLE)) {
            // The tables exist but may have been created by another runtime in which case they have to be opened with
            // key/value/metadata type info
            openExistingTable(LOG_REPLICATION_PLUGIN_VERSION_TABLE);
            openExistingTable(LOG_REPLICATION_STREAMS_INFO_TABLE);
            if (!tableVersionMatchesPlugin()) {
                // delete the tables and recreate them
                deleteExistingStreamNameAndVersionTables();
                createStreamNameAndVersionTables(this.readStreamsToReplicateFromRegistry(this.runtime.getAddressSpaceView().getLogTail()));
            }
        } else {
            // If any 1 of the 2 tables does not exist, delete and recreate them both as they may have been corrupted.
            deleteExistingStreamNameAndVersionTables();
            createStreamNameAndVersionTables(this.readStreamsToReplicateFromRegistry(this.runtime.getAddressSpaceView().getLogTail()));
        }
        return readStreamsToReplicateFromTable();
    }

    public void addStreamsToInfoTable(Set<UUID> streamIdSet) {
        try {
            Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    LOG_REPLICATION_STREAMS_INFO_TABLE,
                    TableInfo.class, Namespace.class, CommonTypes.Uuid.class,
                    TableOptions.builder().build());

            addStreamsToInfoTableWithRetry(streamIdSet, streamsInfoTable);
        } catch (Exception e) {
            log.warn("Exception when opening LR stream info table ", e);
        }
    }

    private void addStreamsToInfoTableWithRetry(Set<UUID> streamIdSet,
                                                Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                addStreamsToInfoTable(streamIdSet, streamsInfoTable);
                log.debug("Successfully added streams {} to the info table.", streamIdSet);
                return null;
            }).run();
        } catch (InterruptedException ie) {
            log.error("Unrecoverable exception when attempting to add streams.", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }

    private void addStreamsToInfoTable(Set<UUID> streamIdSet,
                                       Table<TableInfo, Namespace, CommonTypes.Uuid> streamsInfoTable)
            throws RetryNeededException {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            for (UUID id : streamIdSet) {
                TableInfo tableInfo = TableInfo.newBuilder().setId(id.toString()).build();
                Namespace namespace = Namespace.newBuilder().setName(EMPTY_STR).build();
                txn.putRecord(streamsInfoTable, tableInfo, namespace, defaultMetadata);
            }
            txn.commit();
        } catch (TransactionAbortedException tae) {
            throw new RetryNeededException();
        }
    }

    public boolean isUpgraded() {
        if (verifyTableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE)) {
            openExistingTable(LOG_REPLICATION_PLUGIN_VERSION_TABLE);
            return !tableVersionMatchesPlugin();
        }
        // TODO pankti: this may be the first time the replication server is initialized, so return false.
        //  But what about a case if the user has deleted the table?
        return false;
    }

    private void initStreamNameFetcherPlugin() {
        log.info(">> PLUGIN IS: {}", pluginConfigFilePath);
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            logReplicationConfigAdapter = (ILogReplicationConfigAdapter) plugin.getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Stream Fetcher Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    private boolean verifyTableExists(String tableName) {
        try {
            corfuStore.getTable(CORFU_SYSTEM_NAMESPACE, tableName);
        } catch (NoSuchElementException e) {
            // Table does not exist
            return false;
        } catch (IllegalArgumentException e) {
            // Ignore
        }
        return true;
    }

    private void openExistingTable(String tableName) {
        try {
            if (Objects.equals(tableName, LOG_REPLICATION_STREAMS_INFO_TABLE)) {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, tableName, TableInfo.class,
                    Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            } else {
                corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, VersionString.class,
                        Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening existing table {}", tableName, e);
        }
    }

    private boolean tableVersionMatchesPlugin() {
        VersionString versionString = VersionString.newBuilder().setName("VERSION").build();
        CorfuStoreEntry<VersionString, Version, CommonTypes.Uuid> record;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            // If the version table is dropped using the CorfuStoreBrowser(UFO) for testing, it will be empty.
            // In this case, it should be re-created
            record = txn.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString);
            txn.commit();
        }

        return record.getPayload() != null && (Objects.equals(record.getPayload().getVersion(),
                logReplicationConfigAdapter.getVersion()));
    }

    private void deleteExistingStreamNameAndVersionTables() {
        try {
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_INFO_TABLE);
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        } catch (NoSuchElementException e) {
            // If the table does not exist, simply return
        }
    }

    private void createStreamNameAndVersionTables(Set<TableInfo> streamInfos) {
        try {
            Table<TableInfo, Namespace, CommonTypes.Uuid> streamInfoTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                LOG_REPLICATION_STREAMS_INFO_TABLE,
                TableInfo.class, Namespace.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

            Table<VersionString, Version, CommonTypes.Uuid> pluginVersionTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                LOG_REPLICATION_PLUGIN_VERSION_TABLE,
                VersionString.class, Version.class, CommonTypes.Uuid.class,
                TableOptions.builder().build());

            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                // Populate the plugin version in the version table
                VersionString versionString = VersionString.newBuilder().setName("VERSION").build();
                Version version = Version.newBuilder().setVersion(logReplicationConfigAdapter.getVersion()).build();
                txn.putRecord(pluginVersionTable, versionString, version, defaultMetadata);

                // As each name is fully qualified, no need to insert the
                // namespace.  Simply insert an empty string there.
                // TODO: Ideally the Namespace protobuf can be removed but it
                //  will involve data migration on upgrade as it is a schema
                //  change
                Namespace namespace = Namespace.newBuilder().setName(EMPTY_STR).build();
                // add registryTable to the stream into table
                String registryTableName = getFullyQualifiedTableName(
                        CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
                txn.putRecord(streamInfoTable, TableInfo.newBuilder().setName(registryTableName).build(),
                        namespace, defaultMetadata);
                // add protobufDescriptorTable to the stream into table
                String protoTableName = getFullyQualifiedTableName(
                        CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
                txn.putRecord(streamInfoTable, TableInfo.newBuilder().setName(protoTableName).build(),
                        namespace, defaultMetadata);
                // Copy all stream names to the stream info table.  Each name is
                // a fully qualified stream name
                for (TableInfo tableInfo : streamInfos) {
                    txn.putRecord(streamInfoTable, tableInfo, namespace, defaultMetadata);
                }
                txn.commit();
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening the table", e);
        }
    }

    private Set<TableInfo> readStreamsToReplicateFromTable() {
        Set<TableInfo> tableInfoSet = new HashSet<>();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            Set<TableInfo> tables = txn.keySet(LOG_REPLICATION_STREAMS_INFO_TABLE);
            tableInfoSet.addAll(tables);
            txn.commit();
        }
        return tableInfoSet;
    }

    public Set<TableInfo> readStreamsToReplicateFromRegistry(long ts) {

        Set<TableInfo> tableInfoSet = new HashSet<>();
        CorfuTable<CorfuStoreMetadata.TableName, CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                        CorfuStoreMetadata.TableMetadata>>
                registryTable = runtime.getTableRegistry().getRegistryTable();
        Token snapshotToken = Token.of(0L, ts);
        runtime.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(snapshotToken)
                .build()
                .begin();
        Set<CorfuStoreMetadata.TableName> tableNameSet = registryTable.keySet();
        for (CorfuStoreMetadata.TableName tableName : tableNameSet) {
            CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                    CorfuStoreMetadata.TableMetadata> tableRecord = registryTable.get(tableName);

            if (tableRecord.getMetadata().getTableOptions().getIsFederated()) {
                TableInfo info = TableInfo.newBuilder()
                        .setName(getFullyQualifiedTableName(tableName))
                        .build();
                tableInfoSet.add(info);
            }
        }
        runtime.getObjectsView().TXEnd();

        return tableInfoSet;
    }

    public static Set<UUID> getMergeOnlyStreamIdList() {
        return MERGE_ONLY_STREAM_ID_LIST;
    }
}
