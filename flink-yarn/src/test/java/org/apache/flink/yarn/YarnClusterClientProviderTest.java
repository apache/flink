/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.UNDEFINED;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link YarnClusterClientProvider}. */
@ExtendWith(TestLoggerExtension.class)
public class YarnClusterClientProviderTest {

    /** Once Yarn app master has been running, it should not retrieve app report again. */
    @Test
    public void testNoLongerRetrieveAppReportOnceAppRunning() {
        final Configuration flinkConf = new Configuration();
        final ApplicationId applicationId =
                ApplicationId.newInstance(System.currentTimeMillis(), 1);

        final ApplicationReportProvider mockAppReportProvider =
                MockApplicationReportProvider.of(applicationId, "localhost", 10);
        assertEquals(0, ((MockApplicationReportProvider) mockAppReportProvider).getInvokeNumber());

        YarnClusterClientProvider provider =
                YarnClusterClientProvider.of(mockAppReportProvider, flinkConf, applicationId);

        provider.getClusterClient();
        assertEquals(1, ((MockApplicationReportProvider) mockAppReportProvider).getInvokeNumber());

        provider.getClusterClient();
        assertEquals(1, ((MockApplicationReportProvider) mockAppReportProvider).getInvokeNumber());
    }

    @Test
    public void testJMHostShouldSetIntoConfig() {
        final Configuration flinkConf = new Configuration();
        final ApplicationId applicationId =
                ApplicationId.newInstance(System.currentTimeMillis(), 1);
        final String host = "localhost";
        final int rpcPort = 10000;
        final ApplicationReportProvider mockAppReportProvider =
                MockApplicationReportProvider.of(applicationId, host, rpcPort);

        YarnClusterClientProvider provider =
                YarnClusterClientProvider.of(mockAppReportProvider, flinkConf, applicationId);

        ClusterClient clusterClient = provider.getClusterClient();

        final Configuration clusterClientConf = clusterClient.getFlinkConfiguration();
        assertEquals(clusterClientConf.get(JobManagerOptions.ADDRESS), host);
        assertEquals(clusterClientConf.getInteger(JobManagerOptions.PORT), rpcPort);
        assertEquals(clusterClientConf.get(RestOptions.ADDRESS), host);
        assertEquals(clusterClientConf.getInteger(RestOptions.PORT), rpcPort);
        assertEquals(
                clusterClientConf.get(YarnConfigOptions.APPLICATION_ID),
                ConvertUtils.convert(applicationId));
    }

    private static class MockApplicationReportProvider implements ApplicationReportProvider {
        private int invokeNumber = 0;
        private ApplicationId applicationId;
        private String host;
        private int rpcHost;

        public MockApplicationReportProvider(
                ApplicationId applicationId, String host, int rpcHost) {
            this.applicationId = applicationId;
            this.host = host;
            this.rpcHost = rpcHost;
        }

        @Override
        public ApplicationReport waitUntilSubmissionFinishes() throws FlinkException {
            invokeNumber++;
            return buildMockedAppReport(applicationId, YarnApplicationState.RUNNING, host, rpcHost);
        }

        public int getInvokeNumber() {
            return invokeNumber;
        }

        static MockApplicationReportProvider of(
                ApplicationId applicationId, String host, int rpcPort) {
            return new MockApplicationReportProvider(applicationId, host, rpcPort);
        }
    }

    private static ApplicationReport buildMockedAppReport(
            ApplicationId appID, YarnApplicationState appState, String host, int rpcPort) {
        ApplicationReport applicationReport = Records.newRecord(ApplicationReport.class);
        applicationReport.setApplicationId(appID);
        applicationReport.setCurrentApplicationAttemptId(
                ApplicationAttemptId.newInstance(appID, 0));
        applicationReport.setUser("user");
        applicationReport.setQueue("queue");
        applicationReport.setName("name");
        applicationReport.setHost(host);
        applicationReport.setRpcPort(rpcPort);
        applicationReport.setYarnApplicationState(appState);
        applicationReport.setStartTime(1L);
        applicationReport.setFinishTime(2L);
        applicationReport.setFinalApplicationStatus(UNDEFINED);
        applicationReport.setProgress(1.0f);
        return applicationReport;
    }
}
