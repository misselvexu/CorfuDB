package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.send.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;

import java.util.List;
import java.util.UUID;

/**
 *  Empty Implementation of Snapshot Listener - used for state machine transition testing (no logic)
 */
public class EmptyDataSender implements DataSender {
    @Override
    public boolean send(DataMessage message, UUID snapshotSyncId, boolean completed) {
        return true;
    }

    @Override
    public boolean send(List<DataMessage> messages, UUID snapshotSyncId, boolean completed) {
        return true;
    }

    @Override
    public boolean send(DataMessage message) { return true; }

    @Override
    public boolean send(List<DataMessage> messages) { return true; }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) {}

    @Override
    public void onError(LogReplicationError error) {}
}
