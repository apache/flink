package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.List;

public class CheckpointAwaitingSource<T extends Serializable>
        implements SourceFunction<T>, CheckpointListener, CheckpointedFunction {
    private volatile boolean allDataEmitted = false;
    private volatile boolean dataCheckpointed = false;
    private volatile boolean running = true;
    private volatile long checkpointAfterData = -1L;
    private final List<T> data;

    public CheckpointAwaitingSource(List<T> data) {
        this.data = data;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        for (T datum : data) {
            ctx.collect(datum);
        }
        allDataEmitted = true;
        while (!dataCheckpointed && running) {
            Thread.yield();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (checkpointId == this.checkpointAfterData) {
            dataCheckpointed = true;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        if (allDataEmitted) {
            checkpointAfterData = context.getCheckpointId();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}
}
