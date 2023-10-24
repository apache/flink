package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.OptionalLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class InitContextBase implements InitContext {

    private final OptionalLong restoredCheckpointId;

    private final StreamingRuntimeContext runtimeContext;

    public InitContextBase(
            StreamingRuntimeContext runtimeContext, OptionalLong restoredCheckpointId) {
        this.runtimeContext = checkNotNull(runtimeContext);
        this.restoredCheckpointId = restoredCheckpointId;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return runtimeContext.getNumberOfParallelSubtasks();
    }

    @Override
    public int getAttemptNumber() {
        return runtimeContext.getAttemptNumber();
    }

    @Override
    public int getSubtaskId() {
        return runtimeContext.getIndexOfThisSubtask();
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return restoredCheckpointId;
    }

    @Override
    public JobID getJobId() {
        return runtimeContext.getJobId();
    }

    RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}
