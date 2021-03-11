package org.apache.flink.connectors.test.common.environment;

import org.apache.flink.core.execution.JobClient;

/** Interface for triggering failover in a Flink cluster. */
public interface ClusterControllable {

    void triggerJobManagerFailover(JobClient jobClient, Runnable afterFailAction) throws Exception;

    void triggerTaskManagerFailover(JobClient jobClient, Runnable afterFailAction) throws Exception;

    void isolateNetwork(JobClient jobClient, Runnable afterFailAction) throws Exception;
}
