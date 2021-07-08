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

package org.apache.flink.yarn.cli;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Utility class which monitors the specified yarn application status periodically. */
public class YarnApplicationStatusMonitor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationStatusMonitor.class);

    private static final long UPDATE_INTERVAL = 1000L;

    private final YarnClient yarnClient;

    private final ApplicationId yarnApplicationId;

    private final ScheduledFuture<?> applicationStatusUpdateFuture;

    private volatile ApplicationStatus applicationStatus;

    public YarnApplicationStatusMonitor(
            YarnClient yarnClient,
            ApplicationId yarnApplicationId,
            ScheduledExecutor scheduledExecutor) {
        this.yarnClient = Preconditions.checkNotNull(yarnClient);
        this.yarnApplicationId = Preconditions.checkNotNull(yarnApplicationId);

        applicationStatusUpdateFuture =
                scheduledExecutor.scheduleWithFixedDelay(
                        this::updateApplicationStatus, 0L, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);

        applicationStatus = ApplicationStatus.UNKNOWN;
    }

    public ApplicationStatus getApplicationStatusNow() {
        return applicationStatus;
    }

    @Override
    public void close() {
        applicationStatusUpdateFuture.cancel(false);
    }

    private void updateApplicationStatus() {
        if (yarnClient.isInState(Service.STATE.STARTED)) {
            final ApplicationReport applicationReport;

            try {
                applicationReport = yarnClient.getApplicationReport(yarnApplicationId);
            } catch (Exception e) {
                LOG.info(
                        "Could not retrieve the Yarn application report for {}.",
                        yarnApplicationId);
                applicationStatus = ApplicationStatus.UNKNOWN;
                return;
            }

            YarnApplicationState yarnApplicationState = applicationReport.getYarnApplicationState();

            if (yarnApplicationState == YarnApplicationState.FAILED
                    || yarnApplicationState == YarnApplicationState.KILLED) {
                applicationStatus = ApplicationStatus.FAILED;
            } else {
                applicationStatus = ApplicationStatus.SUCCEEDED;
            }
        } else {
            LOG.info(
                    "Yarn client is no longer in state STARTED. Stopping the Yarn application status monitor.");
            applicationStatusUpdateFuture.cancel(false);
        }
    }
}
