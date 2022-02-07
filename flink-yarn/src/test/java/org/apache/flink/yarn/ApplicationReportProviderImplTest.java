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

import org.apache.flink.util.TestLoggerExtension;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.flink.yarn.TestingYarnClient.createApplicationReport;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link ApplicationReportProviderImpl}. */
@ExtendWith(TestLoggerExtension.class)
public class ApplicationReportProviderImplTest {

    /**
     * When the state of Yarn app master is RUNNING in {@link YarnClusterDescriptor}, the {@link
     * ApplicationReportProvider} will not retrieve app report again and return directly.
     */
    @Test
    public void testReturnAppReportDirectlyFromAppRunning() throws Exception {
        final ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 10);
        final ApplicationReport initialAppRunningReport =
                createApplicationReport(
                        appId, YarnApplicationState.RUNNING, FinalApplicationStatus.UNDEFINED);

        final YarnClient yarnClient =
                new NoSupportedYarnClient(Collections.singletonMap(appId, initialAppRunningReport));
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        final ApplicationReportProvider provider =
                ApplicationReportProviderImpl.of(
                        YarnClientRetrieverImpl.from(
                                YarnClientWrapper.fromBorrowed(yarnClient), null),
                        initialAppRunningReport);
        final ApplicationReport report = provider.waitUntilSubmissionFinishes();

        assertEquals(YarnApplicationState.RUNNING, report.getYarnApplicationState());
    }

    /**
     * When Yarn app master has been accepted in {@link YarnClusterDescriptor} and then retrieving
     * app report from {@link ApplicationReportProviderImpl}, it should pass and the returned app's
     * state should be running.
     */
    @Test
    public void testReturnAppReportFromAppAccepted() throws Exception {
        final ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 10);
        final ApplicationReport initialAppAcceptedReport =
                createApplicationReport(
                        appId, YarnApplicationState.ACCEPTED, FinalApplicationStatus.UNDEFINED);

        final ApplicationReport submissionFinishedAppReport =
                createApplicationReport(
                        appId, YarnApplicationState.RUNNING, FinalApplicationStatus.UNDEFINED);
        final YarnClient yarnClient =
                new TestingYarnClient(Collections.singletonMap(appId, submissionFinishedAppReport));

        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        final ApplicationReportProvider provider =
                ApplicationReportProviderImpl.of(
                        YarnClientRetrieverImpl.from(
                                YarnClientWrapper.fromBorrowed(yarnClient), null),
                        initialAppAcceptedReport);
        final ApplicationReport report = provider.waitUntilSubmissionFinishes();

        assertEquals(YarnApplicationState.RUNNING, report.getYarnApplicationState());
    }

    private class NoSupportedYarnClient extends TestingYarnClient {

        protected NoSupportedYarnClient(Map<ApplicationId, ApplicationReport> applicationReports) {
            super(applicationReports);
        }

        @Override
        public ApplicationReport getApplicationReport(ApplicationId appId)
                throws YarnException, IOException {
            throw new YarnException("GetApplicationReport is not supported.");
        }
    }
}
