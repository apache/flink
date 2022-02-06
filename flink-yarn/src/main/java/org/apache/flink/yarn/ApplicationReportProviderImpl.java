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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkException;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;

import javax.annotation.Nullable;

import java.io.IOException;

/** The implementation of {@link ApplicationReportProvider}. */
@Internal
public class ApplicationReportProviderImpl implements ApplicationReportProvider {
    private final YarnClientRetriever yarnClientRetriever;
    private final ApplicationId appId;
    @Nullable private ApplicationReport submissionFinishedAppReport;

    private ApplicationReportProviderImpl(
            YarnClientRetriever yarnClientRetriever, ApplicationReport submissionAppReport) {
        this.yarnClientRetriever = yarnClientRetriever;
        this.appId = submissionAppReport.getApplicationId();
        if (submissionAppReport.getYarnApplicationState() == YarnApplicationState.RUNNING) {
            this.submissionFinishedAppReport = submissionAppReport;
        }
    }

    @Override
    public ApplicationReport waitUntilSubmissionFinishes() throws FlinkException {
        if (submissionFinishedAppReport != null) {
            return submissionFinishedAppReport;
        }

        try (final YarnClientWrapper yarnClient = yarnClientRetriever.getYarnClient()) {
            return YarnClusterDescriptor.waitUntilTargetState(
                    yarnClient, appId, YarnApplicationState.RUNNING);
        } catch (YarnException | IOException e) {
            throw new FlinkException(
                    "Errors on getting YARN application report. Maybe application has finished.",
                    e);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new FlinkException(
                    "Errors on getting YARN application report. Maybe the thread is interrupted.",
                    interruptedException);
        } catch (Exception exception) {
            throw new FlinkException("Errors on closing YarnClient.", exception);
        }
    }

    static ApplicationReportProviderImpl of(
            YarnClientRetriever retriever, ApplicationReport submissionAppReport) {
        return new ApplicationReportProviderImpl(retriever, submissionAppReport);
    }
}
