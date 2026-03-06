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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configuration options for the {@link ApplicationResultStore}. */
public class ApplicationResultStoreOptions {

    public static final ConfigOption<String> STORAGE_PATH =
            ConfigOptions.key("application-result-store.storage-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The path where the application result store stores its data. If not explicitly configured, "
                                    + "it defaults to the value of high-availability.storagePath/application-result-store/<cluster-id>");

    public static final ConfigOption<Boolean> DELETE_ON_COMMIT =
            ConfigOptions.key("application-result-store.delete-on-commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to delete application results from the application result store after they have been committed. "
                                    + "If set to false, the application results will be moved to a clean state instead of being deleted.");

    private ApplicationResultStoreOptions() {}
}
