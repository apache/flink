/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.apache.flink.yarn.YarnTestUtils.isHadoopVersionGreaterThanOrEquals;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/** Tests to Yarn's priority scheduling. */
public class YarnPrioritySchedulingITCase extends YarnTestBase {

    @BeforeClass
    public static void setup() {
        assumeTrue(
                "Priority scheduling is not supported by Hadoop: " + VersionInfo.getVersion(),
                isHadoopVersionGreaterThanOrEquals(2, 8));

        YARN_CONFIGURATION.setStrings("yarn.cluster.max-application-priority", "10");
        startYARNWithConfig(YARN_CONFIGURATION);
    }

    @Test
    public void yarnApplication_submissionWithPriority_shouldRespectPriority() throws Exception {
        runTest(
                () -> {
                    final int priority = 5;
                    final Runner yarnSessionClusterRunner =
                            startWithArgs(
                                    new String[] {
                                        "-j",
                                        flinkUberjar.getAbsolutePath(),
                                        "-t",
                                        flinkLibFolder.getAbsolutePath(),
                                        "-jm",
                                        "768m",
                                        "-tm",
                                        "1024m",
                                        "-Dyarn.application.priority=" + priority
                                    },
                                    "JobManager Web Interface:",
                                    RunTypes.YARN_SESSION);

                    try {
                        final ApplicationReport applicationReport = getOnlyApplicationReport();

                        assertApplicationIsStartedWithPriority(applicationReport, priority);
                    } finally {
                        yarnSessionClusterRunner.sendStop();
                        yarnSessionClusterRunner.join();
                    }
                });
    }

    private void assertApplicationIsStartedWithPriority(
            ApplicationReport applicationReport, int priority) throws Exception {
        final String getPriorityMethodName = "getPriority";

        final Class<? extends ApplicationReport> applicationReportClass =
                applicationReport.getClass();
        final Method getPriorityMethod = applicationReportClass.getMethod(getPriorityMethodName);
        final Object priorityResult = getPriorityMethod.invoke(applicationReport);

        final Class<?> priorityClass = priorityResult.getClass();
        final Method getPriorityPriorityMethod = priorityClass.getMethod(getPriorityMethodName);

        assertThat(getPriorityPriorityMethod.invoke(priorityResult), is(priority));
    }
}
