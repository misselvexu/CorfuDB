/* *****************************************************************************
 * Copyright (c) 2016-2019. VMware, Inc.  All rights reserved. VMware Confidential
 * ****************************************************************************/
package org.corfudb.compactor;

import com.google.common.reflect.TypeToken;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.util.serializer.Serializers;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

@Slf4j
public class CorfuRuntimeHelper {

    private CorfuRuntime corfuRuntime;

    private static final String NODE_UUID_PATH = "/common/configs/serial_number";
    private static final String UUID_KEY = "serial";
    private static final String NODE_UUID_PREFIX = UUID_KEY + "=";

    private static int maxWriteSize = Integer.MAX_VALUE;

    private static int bulkReadSize = 10;

    private static String runtimeKeyStore;
    private static String runtimeKeystorePasswordFile;
    private static String runtimeTrustStore;
    private static String runtimeTrustStorePasswordFile;
    private static boolean enableTls = false;

    public static final String NODE_TOKEN = "node-token";
    public static final String CHECKPOINT = "checkpoint";

    private static final int systemDownHandlerTriggerLimit = 100;  // Corfu default is 20
    private static final Runnable defaultSystemDownHandler = new Runnable(){
        @Override
        public void run() {
            throw new UnreachableClusterException("Cluster is unavailable");
        }
    };

    CorfuRuntimeHelper(List<String> hostnames, int port, int maxWriteSize, int bulkReadSize) {
        CorfuRuntimeHelper.enableTls = false;
        log.info("Set maxWriteSize to {}, bulkReadSize to {}, no tls", maxWriteSize, bulkReadSize);
        connectCorfuRuntime(hostnames, port);
    }

    CorfuRuntimeHelper(List<String> hostnames, int port, int maxWriteSize, int bulkReadSize,
                       String runtimeKeyStore,
                       String runtimeKeystorePasswordFile,
                       String runtimeTrustStore,
                       String runtimeTrustStorePasswordFile) {
        CorfuRuntimeHelper.maxWriteSize = maxWriteSize;
        CorfuRuntimeHelper.bulkReadSize = bulkReadSize;
        CorfuRuntimeHelper.runtimeKeyStore = runtimeKeyStore;
        CorfuRuntimeHelper.runtimeKeystorePasswordFile = runtimeKeystorePasswordFile;
        CorfuRuntimeHelper.runtimeTrustStore = runtimeTrustStore;
        CorfuRuntimeHelper.runtimeTrustStorePasswordFile = runtimeKeystorePasswordFile;
        CorfuRuntimeHelper.enableTls = true;
        log.info("Set maxWriteSize to {}, bulkReadSize to {} with TLS", maxWriteSize, bulkReadSize);
        connectCorfuRuntime(hostnames, port);
    }

    private void connectCorfuRuntime(List<String> hostnames, int port) {
        CorfuRuntimeParameters params = buildCorfuRuntimeParameters();
        String connectionString = constructConnectionString(hostnames, port);
        corfuRuntime = CorfuRuntime.fromParameters(params);
        corfuRuntime.parseConfigurationString(connectionString).connect();
        log.info("Successfully connected to {}", hostnames.toString());
    }

    private CorfuRuntimeParameters buildCorfuRuntimeParameters() {
        CorfuRuntimeParametersBuilder builder = CorfuRuntimeParameters.builder()
            .cacheDisabled(true)
            .priorityLevel(PriorityLevel.HIGH)
            .maxWriteSize(maxWriteSize)
            .bulkReadSize(bulkReadSize)
            .systemDownHandler(defaultSystemDownHandler)
            .systemDownHandlerTriggerLimit(systemDownHandlerTriggerLimit);

        if (enableTls) {
            enableCorfuTls(builder);
        } else {
            builder.tlsEnabled(false);
        }

        return builder.build();
    }

    private void enableCorfuTls(CorfuRuntimeParametersBuilder corfuRuntimeParametersBuilder) {
        corfuRuntimeParametersBuilder
            .tlsEnabled(true)
            .keyStore(runtimeKeyStore)
            .ksPasswordFile(runtimeKeystorePasswordFile)
            .trustStore(runtimeTrustStore)
            .tsPasswordFile(runtimeTrustStorePasswordFile);
    }

    CorfuRuntime getRuntime() {
        return corfuRuntime;
    }

    public static CorfuTable<String, Token> getCheckpointMap(CorfuRuntime corfuRuntime) {
        return corfuRuntime.getObjectsView()
                .build()
                .setStreamName(CHECKPOINT)
                .setTypeToken(new TypeToken<CorfuTable<String, Token>>() {})
                .setSerializer(Serializers.JSON)
                .open();
    }

    static CorfuTable<String, Token> getNodeTrimTokenMap(CorfuRuntime corfuRuntime) {
        return corfuRuntime.getObjectsView()
                .build()
                .setStreamName(NODE_TOKEN)
                .setTypeToken(new TypeToken<CorfuTable<String, Token>>() {})
                .setSerializer(Serializers.JSON)
                .open();
    }

    /**
     * Read the UUID of this node from UUID file.
     *
     * @return UUID of this node.
     */
    static String getThisNodeUuid() throws Exception {
        File f = new File(NODE_UUID_PATH);
        List<String> lines = Files.readAllLines(f.toPath());
        if (lines.size() != 1) {
            throw new RuntimeException("No serial number found in " + NODE_UUID_PATH);
        }

        String nodeIdString = lines.get(0);

        if (!nodeIdString.startsWith(NODE_UUID_PREFIX)) {
            throw new RuntimeException("No serial number found in " + NODE_UUID_PATH);
        }

        nodeIdString = nodeIdString.substring(NODE_UUID_PREFIX.length()).trim();

        log.info("Get this node UUID = {}", nodeIdString);
        return nodeIdString;
    }

    private String constructConnectionString(List<String> hostnames, int port) {
        StringBuilder connectionString = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++) {
            connectionString.append(hostnames.get(i) + ":" + port).append(",");
        }
        return connectionString.deleteCharAt(connectionString.length() - 1).toString();
    }
}
