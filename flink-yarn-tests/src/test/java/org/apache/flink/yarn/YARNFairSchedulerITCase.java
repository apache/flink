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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * This test starts a MiniYARNCluster with a FairScheduler, which has a queue called "default" by
 * default. The configuration here adds another queue: "queueA".
 */
public class YARNFairSchedulerITCase extends YarnTestBase {

    @BeforeAll
    static void setup() throws Exception {
        YARN_CONFIGURATION.setClass(
                YarnConfiguration.RM_SCHEDULER, FairScheduler.class, ResourceScheduler.class);
        YARN_CONFIGURATION.set(
                YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-fair-scheduler");

        File fairSchedulerFile = File.createTempFile("fair-scheduler", ".xml");
        YARN_CONFIGURATION.set(
                FairSchedulerConfiguration.ALLOCATION_FILE, fairSchedulerFile.getAbsolutePath());

        PrintWriter out = new PrintWriter(new FileWriter(fairSchedulerFile));
        out.println("<?xml version=\"1.0\"?>");
        out.println("<allocations>");
        out.println("  <queue name=\"queueA\">");
        out.println("   <maxContainerAllocation>512 mb 1 vcores</maxContainerAllocation>");
        out.println("  </queue>");
        out.println("</allocations>");
        out.close();

        startYARNWithConfig(YARN_CONFIGURATION);
    }

    @AfterAll
    static void tearDown() throws Exception {
        YarnTestBase.teardown();
    }

    @Test
    void testCheckYarnQueues() {
        Configuration configuration = new Configuration();
        configuration.setString(YarnConfigOptions.APPLICATION_QUEUE, "root.a");
        YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(configuration);
        Assertions.assertFalse(yarnClusterDescriptor.checkYarnQueues());

        configuration.setString(YarnConfigOptions.APPLICATION_QUEUE, "root.queueA");
        yarnClusterDescriptor = createYarnClusterDescriptor(configuration);
        Assertions.assertTrue(yarnClusterDescriptor.checkYarnQueues());
    }
}
