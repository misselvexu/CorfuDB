package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Builder;
import org.corfudb.runtime.CorfuOptions;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Created by zlokhandwala on 2019-08-09.
 */
@Builder
public class TableOptions {

    /**
     * If this path is set, {@link CorfuStore} will utilize disk-backed {@link CorfuTable}.
     */
    private final Path persistentDataPath;

    /**
     * Capture options like stream tags, backup restore, log replication at Table level
     */
    private final CorfuOptions.SchemaOptions schemaOptions;

    public CorfuOptions.SchemaOptions getSchemaOptions() {
        return schemaOptions;
    }

    public Optional<Path> getPersistentDataPath() {
        return Optional.ofNullable(persistentDataPath);
    }

    /**
     * Helper function to extract corfu table schema options from message
     * and also preserve existing options like persistedPath
     * @param vClass - the java class created from a .proto message definition
     * @param tableOptions - old table options to migrate from
     * @return TableOptions that carry the message options defined within the proto
     */
    public static <V extends Message> TableOptions fromProtoSchema(@Nonnull Class<V> vClass,
                                                                   TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        TableOptions.TableOptionsBuilder tableOptionsBuilder =
                new TableOptions.TableOptionsBuilder();
        if (vClass != null) { // some test cases pass vClass as null to verify behavior
            V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);
            tableOptionsBuilder.schemaOptions(defaultValueMessage
                    .getDescriptorForType()
                    .getOptions()
                    .getExtension(CorfuOptions.tableSchema));
        }
        if (tableOptions != null && tableOptions.getPersistentDataPath().isPresent()) {
            tableOptionsBuilder.persistentDataPath((Path) tableOptions.getPersistentDataPath().get());
        }
        return tableOptionsBuilder.build();
    }

    public static <V extends Message> TableOptions fromProtoSchema(@Nonnull Class<V> vClass)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return fromProtoSchema(vClass, null);
    }
}