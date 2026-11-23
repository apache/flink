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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import static org.apache.flink.configuration.description.TextElement.text;

/** The set of configuration options relating to the Application Result Store. */
@PublicEvolving
public class ApplicationResultStoreOptions {

    @Documentation.Section(Documentation.Sections.COMMON_HIGH_AVAILABILITY_APPLICATION_RESULT_STORE)
    public static final ConfigOption<StorageType> STORAGE_TYPE =
            ConfigOptions.key("application-result-store.storage-type")
                    .enumType(StorageType.class)
                    .defaultValue(StorageType.FILE_SYSTEM)
                    .withDescription(
                            "Defines where the cluster application results are stored "
                                    + "for persistence across leadership changes.");

    @Documentation.Section(Documentation.Sections.COMMON_HIGH_AVAILABILITY_APPLICATION_RESULT_STORE)
    public static final ConfigOption<String> STORAGE_PATH =
            ConfigOptions.key("application-result-store.storage-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines where application results should be stored. This should be an "
                                                    + "underlying file-system that provides read-after-write consistency. By "
                                                    + "default, this is %s. Only applies if %s is %s.",
                                            TextElement.code(
                                                    FileSystemApplicationResultStore
                                                            .createDefaultApplicationResultStorePath(
                                                                    String.format(
                                                                            "{%s}",
                                                                            HighAvailabilityOptions
                                                                                    .HA_STORAGE_PATH
                                                                                    .key()),
                                                                    String.format(
                                                                            "{%s}",
                                                                            HighAvailabilityOptions
                                                                                    .HA_CLUSTER_ID
                                                                                    .key()))),
                                            TextElement.code(STORAGE_TYPE.key()),
                                            TextElement.code(StorageType.FILE_SYSTEM.name()))
                                    .build());

    @Documentation.Section(Documentation.Sections.COMMON_HIGH_AVAILABILITY_APPLICATION_RESULT_STORE)
    public static final ConfigOption<Boolean> DELETE_ON_COMMIT =
            ConfigOptions.key("application-result-store.delete-on-commit")
                    .booleanType()
                    .defaultValue(Boolean.TRUE)
                    .withDescription(
                            "Determines whether application results should be automatically removed "
                                    + "from the underlying application result store when the corresponding entity "
                                    + "transitions into a clean state. If false, the cleaned application results "
                                    + "are, instead, marked as clean to indicate their state. In this "
                                    + "case, Flink no longer has ownership and the resources need to "
                                    + "be cleaned up by the user.");

    /** The storage backing the application result store. */
    public enum StorageType implements DescribedEnum {
        FILE_SYSTEM(text("Entries are stored as files under the configured storage path.")),
        KUBERNETES(
                text(
                        "Entries are stored in a Kubernetes ConfigMap created by the cluster. "
                                + "Requires %s to be %s.",
                        TextElement.code(HighAvailabilityOptions.HA_MODE.key()),
                        TextElement.code(HighAvailabilityMode.KUBERNETES.name())));

        private final InlineElement description;

        StorageType(InlineElement description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
