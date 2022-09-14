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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

/** The set of configuration options relating to the Job Result Store. */
public class JobResultStoreOptions {
    @Documentation.Section(Documentation.Sections.COMMON_HIGH_AVAILABILITY_JOB_RESULT_STORE)
    public static final ConfigOption<String> STORAGE_PATH =
            ConfigOptions.key("job-result-store.storage-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines where job results should be stored. This should be an "
                                                    + "underlying file-system that provides read-after-write consistency. By "
                                                    + "default, this is %s.",
                                            TextElement.code(
                                                    FileSystemJobResultStore
                                                            .createDefaultJobResultStorePath(
                                                                    String.format(
                                                                            "{%s}",
                                                                            HighAvailabilityOptions
                                                                                    .HA_STORAGE_PATH
                                                                                    .key()),
                                                                    String.format(
                                                                            "{%s}",
                                                                            HighAvailabilityOptions
                                                                                    .HA_CLUSTER_ID
                                                                                    .key()))))
                                    .build());

    @Documentation.Section(Documentation.Sections.COMMON_HIGH_AVAILABILITY_JOB_RESULT_STORE)
    public static final ConfigOption<Boolean> DELETE_ON_COMMIT =
            ConfigOptions.key("job-result-store.delete-on-commit")
                    .booleanType()
                    .defaultValue(Boolean.TRUE)
                    .withDescription(
                            "Determines whether job results should be automatically removed "
                                    + "from the underlying job result store when the corresponding entity "
                                    + "transitions into a clean state. If false, the cleaned job results "
                                    + "are, instead, marked as clean to indicate their state. In this "
                                    + "case, Flink no longer has ownership and the resources need to "
                                    + "be cleaned up by the user.");
}
