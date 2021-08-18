package org.corfudb.runtime.collections;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.RemoteCorfuTableAdapter;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RemoteCorfuTable<K,V> implements ICorfuTable<K,V>, AutoCloseable {
    private final RemoteCorfuTableAdapter<K,V> adapter;
    private final String tableName;
    private final UUID streamId;

    @Override
    public void insert(K key, V value) {
        adapter.update(key, value);
    }

    @Override
    public void delete(K key) {
        adapter.delete(key, adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> scanFromBeginning() {
        return adapter.scan(adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> scanFromBeginning(int numEntries) {
        return adapter.scan(numEntries, adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> cursorScan(K startPoint) {
        return adapter.scan(startPoint, adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> cursorScan(K startPoint, int numEntries) {
        return adapter.scan(startPoint, numEntries, adapter.getCurrentTimestamp());
    }


    //TODO: segment the fulldb scan
    @Override
    public List<V> scanAndFilter(Predicate<? super V> valuePredicate) {
        List<RemoteCorfuTableEntry<K,V>> allEntries = adapter.fullDatabaseScan(adapter.getCurrentTimestamp());
        return allEntries.stream()
                .filter(entry -> valuePredicate.test(entry.getValue()))
                .map(RemoteCorfuTableEntry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {
        List<RemoteCorfuTableEntry<K,V>> allEntries = adapter.fullDatabaseScan(adapter.getCurrentTimestamp());
        return allEntries.stream().filter(entryPredicate).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Stream<Entry<K, V>> entryStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return adapter.size(adapter.getCurrentTimestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return adapter.containsKey((K) key, adapter.getCurrentTimestamp());
    }

    @Override
    public boolean containsValue(Object value) {
        return adapter.containsValue((V) value, adapter.getCurrentTimestamp());
    }

    @Override
    public V get(Object key) {
        return adapter.get((K) key, adapter.getCurrentTimestamp());
    }

    @Override
    public V put(K key, V value) {
        long timestamp = adapter.getCurrentTimestamp();
        //synchronization guarantees unneeded as timestamp will define versioning
        V returnVal = adapter.get((K) key, timestamp);
        adapter.update(key, value);
        return returnVal;
    }

    @Override
    public V remove(Object key) {
        long timestamp = adapter.getCurrentTimestamp();
        //synchronization guarantees unneeded as timestamp will define versioning
        V returnVal = adapter.get((K) key, timestamp);
        adapter.delete((K) key, timestamp);
        return returnVal;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        adapter.updateAll(m, adapter.getCurrentTimestamp());
    }

    @Override
    public void clear() {
        adapter.clear(adapter.getCurrentTimestamp());
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        //TODO: close adapter and deregister table
        adapter.close();
    }

    @AllArgsConstructor
    public static class RemoteCorfuTableEntry<K,V> implements Map.Entry<K,V> {

        private final K key;
        private final V value;

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        /**
         * {@inheritDoc}
         * <p>
         *     Since the data for the table is stored on server side, please perform updates to the
         *     map through update.
         * </p>
         */
        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }

    public static class RemoteCorfuTableFactory<I, J> {
        public RemoteCorfuTable<I,J> openTable(@NonNull CorfuRuntime runtime, @NonNull String tableName) {
            UUID streamID = UUID.nameUUIDFromBytes(tableName.getBytes(StandardCharsets.UTF_8));
            RemoteCorfuTableAdapter<I,J> adapter = new RemoteCorfuTableAdapter<>(tableName, streamID, runtime);
            return new RemoteCorfuTable<>(adapter, tableName, streamID);
        }
    }
}
