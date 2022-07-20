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

import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link YarnClusterDescriptor}. */
class AbstractYarnClusterTest {

    /** Tests that the cluster retrieval of a finished YARN application fails. */
    @Test
    void testClusterClientRetrievalOfFinishedYarnApplication(@TempDir Path tempDir) {

        final ApplicationId applicationId =
                ApplicationId.newInstance(System.currentTimeMillis(), 42);
        final ApplicationReport applicationReport =
                createApplicationReport(
                        applicationId,
                        YarnApplicationState.FINISHED,
                        FinalApplicationStatus.SUCCEEDED);

        final YarnClient yarnClient =
                new TestingYarnClient(Collections.singletonMap(applicationId, applicationReport));
        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        try (YarnClusterDescriptor clusterDescriptor =
                YarnTestUtils.createClusterDescriptorWithLogging(
                        tempDir.toFile().getAbsolutePath(),
                        new Configuration(),
                        yarnConfiguration,
                        yarnClient,
                        false)) {
            assertThatThrownBy(() -> clusterDescriptor.retrieve(applicationId))
                    .isInstanceOf(ClusterRetrieveException.class);
        }
    }

    private ApplicationReport createApplicationReport(
            ApplicationId applicationId,
            YarnApplicationState yarnApplicationState,
            FinalApplicationStatus finalApplicationStatus) {

        ApplicationReport applicationReport = Records.newRecord(ApplicationReport.class);
        applicationReport.setApplicationId(applicationId);
        applicationReport.setCurrentApplicationAttemptId(
                ApplicationAttemptId.newInstance(applicationId, 0));
        applicationReport.setUser("user");
        applicationReport.setQueue("queue");
        applicationReport.setName("name");
        applicationReport.setHost("localhost");
        applicationReport.setRpcPort(42);
        applicationReport.setYarnApplicationState(yarnApplicationState);
        applicationReport.setStartTime(1L);
        applicationReport.setFinishTime(2L);
        applicationReport.setFinalApplicationStatus(finalApplicationStatus);
        applicationReport.setProgress(1.0f);
        return applicationReport;
    }

    private static final class TestingYarnClient extends YarnClientImpl {
        private final Map<ApplicationId, ApplicationReport> applicationReports;

        private TestingYarnClient(Map<ApplicationId, ApplicationReport> applicationReports) {
            this.applicationReports = Preconditions.checkNotNull(applicationReports);
        }

        @Override
        public ApplicationReport getApplicationReport(ApplicationId appId)
                throws YarnException, IOException {
            final ApplicationReport applicationReport = applicationReports.get(appId);

            if (applicationReport != null) {
                return applicationReport;
            } else {
                return super.getApplicationReport(appId);
            }
        }
    }
}
