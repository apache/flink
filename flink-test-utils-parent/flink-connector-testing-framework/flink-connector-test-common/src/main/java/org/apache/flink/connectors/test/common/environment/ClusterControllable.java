package org.apache.flink.connectors.test.common.environment;

/** Interface for triggering failover in a Flink cluster. */
public interface ClusterControllable {

    void triggerJobManagerFailover();

    void triggerTaskManagerFailover();

    void isolateNetwork();
}
