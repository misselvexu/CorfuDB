package org.corfudb.logreplication.fsm;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *  Empty Implementation of Snapshot Listener - used for state machine transition testing (no logic)
 */
public class EmptyDataSender implements DataSender {

    @Override
    public CompletableFuture<LogReplicationEntry> send(LogReplicationEntry message) { return new CompletableFuture<>(); }

    @Override
    public boolean send(List<LogReplicationEntry> messages) { return true; }

    @Override
    public void onError(LogReplicationError error) {}
}
