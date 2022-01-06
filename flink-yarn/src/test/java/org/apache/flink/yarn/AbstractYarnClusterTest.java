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
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;

import static org.apache.flink.yarn.TestingYarnClient.createApplicationReport;

/** Tests for the {@link YarnClusterDescriptor}. */
public class AbstractYarnClusterTest extends TestLogger {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    /** Tests that the cluster retrieval of a finished YARN application fails. */
    @Test(expected = ClusterRetrieveException.class)
    public void testClusterClientRetrievalOfFinishedYarnApplication() throws Exception {
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

        final YarnClusterDescriptor clusterDescriptor =
                YarnTestUtils.createClusterDescriptorWithLogging(
                        temporaryFolder.newFolder().getAbsolutePath(),
                        new Configuration(),
                        yarnConfiguration,
                        yarnClient,
                        false);

        try {
            clusterDescriptor.retrieve(applicationId);
        } finally {
            clusterDescriptor.close();
        }
    }
}
