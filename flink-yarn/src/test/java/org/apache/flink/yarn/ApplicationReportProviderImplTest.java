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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.apache.flink.yarn.TestingYarnClient.createApplicationReport;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link ApplicationReportProviderImpl}. */
@ExtendWith(TestLoggerExtension.class)
public class ApplicationReportProviderImplTest {

    /**
     * When Yarn app master has been accepted in {@link YarnClusterDescriptor} and then retrieving
     * app report from {@link ApplicationReportProviderImpl}, it should pass and the returned app's
     * state should be running.
     */
    @Test
    public void testReturnAppReportFromAppAccepted() throws Exception {
        final ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 10);

        final ApplicationReport applicationReport =
                createApplicationReport(
                        appId, YarnApplicationState.RUNNING, FinalApplicationStatus.UNDEFINED);

        YarnClient yarnClient =
                new TestingYarnClient(Collections.singletonMap(appId, applicationReport));
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        ApplicationReportProvider provider =
                ApplicationReportProviderImpl.of(
                        YarnClientRetrieverImpl.from(YarnClientWrapper.of(yarnClient, true), null),
                        appId);
        ApplicationReport report = provider.waitTillSubmissionFinish();

        assertEquals(YarnApplicationState.RUNNING, report.getYarnApplicationState());
    }
}
