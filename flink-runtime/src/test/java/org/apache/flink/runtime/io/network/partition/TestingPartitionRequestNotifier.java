package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import java.io.IOException;

public class TestingPartitionRequestNotifier implements PartitionRequestNotifier {
    @Override
    public long getCreateTimestamp() {
        return 0;
    }

    @Override
    public ResultPartitionID getResultPartitionId() {
        return null;
    }

    @Override
    public NetworkSequenceViewReader getViewReader() {
        return null;
    }

    @Override
    public InputChannelID getReceiverId() {
        return null;
    }

    @Override
    public void notifyPartitionRequest(ResultPartition partition) throws IOException {

    }

    @Override
    public void notifyPartitionRequestTimeout() {

    }

    @Override
    public void releaseNotifier() {

    }

    public static TestingPartitionRequestNotifierBuilder newBuilder() {
        return new TestingPartitionRequestNotifierBuilder();
    }

    public static class TestingPartitionRequestNotifierBuilder {

        public TestingPartitionRequestNotifier build() {
            return new TestingPartitionRequestNotifier();
        }
    }
}
