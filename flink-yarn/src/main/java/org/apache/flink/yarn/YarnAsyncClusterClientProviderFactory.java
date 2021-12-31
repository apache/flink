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
import org.apache.flink.client.program.AsyncClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import java.util.concurrent.CompletableFuture;

/** A {@link YarnAsyncClusterClientProviderFactory} for a YARN cluster. */
@Internal
public class YarnAsyncClusterClientProviderFactory {

    static AsyncClusterClientProvider from(
            ApplicationReportProvider applicationReportProvider,
            final Configuration flinkConfiguration) {
        return () -> {
            try {
                ApplicationReport report = applicationReportProvider.waitSubmissionFinish();
                return CompletableFuture.completedFuture(
                        new RestClusterClient<ApplicationId>(
                                flinkConfiguration, report.getApplicationId()));
            } catch (Exception e) {
                throw new RuntimeException("Errors on getting cluster client.", e);
            }
        };
    }
}
