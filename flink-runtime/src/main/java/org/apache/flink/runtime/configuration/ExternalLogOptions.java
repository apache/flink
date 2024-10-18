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

package org.apache.flink.runtime.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** {@link ConfigOption}s specific for a single execution of a user program. */
@PublicEvolving
public class ExternalLogOptions {

    /** Timeout for jobs which don't have a job manager as leader assigned. */
    public static final ConfigOption<String> EXTERNAL_LOG_FACTORY_CLASS =
            ConfigOptions.key("external.log.factory.class")
                    .stringType()
                    .defaultValue("org.apache.flink.externalresource.log.DefaultTerminationLog")
                    .withDescription(
                            "External log factory class used to update flink issues.");
}
