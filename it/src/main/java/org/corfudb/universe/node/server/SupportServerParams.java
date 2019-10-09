package org.corfudb.universe.node.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.Node.NodeType;
import org.corfudb.universe.node.server.CorfuServer.Mode;
import org.corfudb.universe.node.server.CorfuServer.Persistence;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Builder
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"logLevel", "stopTimeout"})
@ToString
public class SupportServerParams implements NodeParams {
    private static final Map<NodeType, Integer> PORTS = ImmutableMap.<NodeType, Integer>builder()
            .put(NodeType.METRICS_SERVER, 9090)
            .build();

    @Default
    @NonNull
    @Getter
    private final Set<Integer> metricPorts = new HashSet<>();

    @Default
    @NonNull
    @Getter
    private final Level logLevel = Level.DEBUG;

    @NonNull
    @Getter
    private final NodeType nodeType;

    @Getter
    @NonNull
    private final String clusterName;

    @Getter
    @Default
    @NonNull
    private final Duration stopTimeout = Duration.ofSeconds(1);

    @Override
    public String getName() {
        return clusterName + "-support-node-" + getNodeType();
    }

    public Set<Integer> getPorts() {
        return ImmutableSet.of(PORTS.get(getNodeType()));
    }

}