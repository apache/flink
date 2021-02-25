package org.apache.flink.connectors.test.common.environment;

import org.apache.flink.api.common.JobID;

/** Interface for triggering failover in a Flink cluster. */
public interface ClusterControllable {

    void triggerJobManagerFailover(JobID jobID, Runnable afterFailAction) throws Exception;

    void triggerTaskManagerFailover(JobID jobID, Runnable afterFailAction) throws Exception;

    void isolateNetwork(Runnable afterFailAction) throws Exception;
}
