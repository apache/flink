package org.apache.flink.tests.util.flink;

import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.AutoCloseableAsync;

import java.io.IOException;
import java.time.Duration;

/** Controller for interacting with a SqlGateway. */
public interface GatewayController extends AutoCloseableAsync {

    /**
     * Submits the given SQL job to the cluster.
     *
     * @param job job to submit.
     * @param timeout the maximum time to wait.
     * @throws IOException if any IO error happen.
     */
    void submitSQLJob(SQLJobSubmission job, Duration timeout) throws Exception;
}
