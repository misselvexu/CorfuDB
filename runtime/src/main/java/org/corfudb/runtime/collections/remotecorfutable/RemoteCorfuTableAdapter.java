package org.corfudb.runtime.collections.remotecorfutable;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.CLEAR;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.DELETE;
import static org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods.UPDATE;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class connects the client API from the RemoteCorfuTable to the data stored on the server.
 *
 * Created by nvaishampayan517 on 08/17/21
 * @param <K> The key type
 * @param <V> The value type
 */
public class RemoteCorfuTableAdapter<K,V> {
    public static final int DEFAULT_SCAN_SIZE = 5;
    private final String tableName;
    private final UUID streamId;
    private final CorfuRuntime runtime;
    private final ISerializer serializer;
    private final IStreamView streamView;


    public RemoteCorfuTableAdapter(@NonNull String tableName, @NonNull UUID streamId, @NonNull CorfuRuntime runtime,
                                   @NonNull ISerializer serializer, IStreamView streamView) {

        if(!UUID.nameUUIDFromBytes(tableName.getBytes(StandardCharsets.UTF_8)).equals(streamId)) {
            throw new IllegalArgumentException("Stream Id must be derived from tableName");
        }
        this.tableName = tableName;
        this.streamId = streamId;
        this.runtime = runtime;
        this.serializer = serializer;
        this.streamView = streamView;
        //TODO: add logic to register table
    }

    public void close() {
        //TODO: add logic to deregister table
    }

    public void clear(long currentTimestamp) {
        Object[] smrArgs = new Object[1];
        smrArgs[0] = currentTimestamp;
        SMREntry entry = new SMREntry(CLEAR.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public void updateAll(Collection<RemoteCorfuTable.RemoteCorfuTableEntry<K,V>> entries,
                          long timestamp) {
        Object[] smrArgs = new Object[2*entries.size() + 1];
        smrArgs[0] = timestamp;
        int i = 1;
        for (RemoteCorfuTable.RemoteCorfuTableEntry<K,V> entry: entries) {
            smrArgs[2*i -1] = entry.getKey();
            smrArgs[2*i] = entry.getValue();
            i++;
        }
        SMREntry entry = new SMREntry(UPDATE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public long getCurrentTimestamp() {
        //use global log tail as timestamp
        return runtime.getAddressSpaceView().getLogTail();
    }

    public V get(K key, long timestamp) {
        ByteString databaseKeyString = serializeObject(key);
        RemoteCorfuTableVersionedKey databaseKey = new RemoteCorfuTableVersionedKey(databaseKeyString, timestamp);
        ByteString databaseValueString = runtime.getRemoteCorfuTableView().get(databaseKey, streamId);
        return (V) deserializeObject(databaseKeyString);
    }

    private ByteString serializeObject(Object payload) {
        ByteBuf serializationBuffer = Unpooled.buffer();
        serializer.serialize(payload, serializationBuffer);
        byte[] intermediaryBuffer = new byte[serializationBuffer.readableBytes()];
        serializationBuffer.readBytes(intermediaryBuffer);
        return ByteString.copyFrom(intermediaryBuffer);
    }

    private Object deserializeObject(ByteString serialiaedOject) {
        byte[] deserializationWrapped = new byte[serialiaedOject.size()];
        serialiaedOject.copyTo(deserializationWrapped, 0);
        ByteBuf deserializationBuffer = Unpooled.wrappedBuffer(deserializationWrapped);
        return serializer.deserialize(deserializationBuffer, runtime);
    }


    public void delete(K key, long timestamp) {
        Object[] smrArgs = new Object[2];
        smrArgs[0] = timestamp;
        smrArgs[1] = key;
        SMREntry entry = new SMREntry(DELETE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public void multiDelete(List<K> keys, long currentTimestamp) {
        Object[] smrArgs = new Object[keys.size()+1];
        smrArgs[0] = currentTimestamp;
        int i = 1;
        for (K key : keys) {
            smrArgs[i] = key;
            i++;
        }
        SMREntry entry = new SMREntry(DELETE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public void update(K key, V value, long timestamp) {
        Object[] smrArgs = new Object[3];
        smrArgs[0] = timestamp;
        smrArgs[1] = key;
        smrArgs[2] = value;
        SMREntry entry = new SMREntry(UPDATE.getSMRName(), smrArgs, serializer);
        ByteBuf serializedEntry = Unpooled.buffer();
        entry.serialize(serializedEntry);
        //TODO: run unit tests to determine errors that can propogate
        streamView.append(serializedEntry);
    }

    public boolean containsValue(V value, long currentTimestamp) {
        ByteString databaseValueString = serializeObject(value);
        return runtime.getRemoteCorfuTableView()
                .containsValue(databaseValueString,streamId,currentTimestamp, DEFAULT_SCAN_SIZE);
    }

    public boolean containsKey(K key, long currentTimestamp) {
        ByteString databaseKeyString = serializeObject(key);
        RemoteCorfuTableVersionedKey databaseKey =
                new RemoteCorfuTableVersionedKey(databaseKeyString,currentTimestamp);
        return runtime.getRemoteCorfuTableView().containsKey(databaseKey,streamId);
    }

    public int size(long currentTimestamp) {
        return runtime.getRemoteCorfuTableView().size(streamId, currentTimestamp, DEFAULT_SCAN_SIZE);
    }


    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(K startPoint, int numEntries, long currentTimestamp) {
        ByteString databaseKeyString = serializeObject(startPoint);
        RemoteCorfuTableVersionedKey databaseKey =
                new RemoteCorfuTableVersionedKey(databaseKeyString,currentTimestamp);
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(databaseKey,
                numEntries, streamId, currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(int numEntries, long currentTimestamp) {
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(numEntries,
                streamId, currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }
    
    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(K startPoint, long currentTimestamp) {
        ByteString databaseKeyString = serializeObject(startPoint);
        RemoteCorfuTableVersionedKey databaseKey =
                new RemoteCorfuTableVersionedKey(databaseKeyString,currentTimestamp);
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(databaseKey,
                streamId, currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }

    public List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> scan(long currentTimestamp) {
        List<RemoteCorfuTableDatabaseEntry> scannedEntries = runtime.getRemoteCorfuTableView().scan(streamId,
                currentTimestamp);
        return getRemoteCorfuTableEntries(scannedEntries);
    }

    private List<RemoteCorfuTable.RemoteCorfuTableEntry<K, V>> getRemoteCorfuTableEntries(List<RemoteCorfuTableDatabaseEntry> scannedEntries) {
        return scannedEntries.stream()
                .map(dbEntry -> new RemoteCorfuTable.RemoteCorfuTableEntry<K,V>(
                        (K) deserializeObject(dbEntry.getKey().getEncodedKey()),
                        (V) deserializeObject(dbEntry.getValue())
                ))
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "RemoteCorfuTableAdapter{" +
                "tableName='" + tableName + '\'' +
                ", streamId=" + streamId +
                '}';
    }


}