package org.apache.flink.connectors.test.common;

/** Test resources. */
public interface TestResource {

    void startUp() throws Exception;

    void tearDown() throws Exception;
}
