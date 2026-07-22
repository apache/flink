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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options for MDC (Mapped Diagnostic Context) enrichment. */
@PublicEvolving
public final class MdcOptions {

    /**
     * Maps job configuration keys to MDC key names. Keys absent or blank in the job configuration
     * are skipped.
     */
    @PublicEvolving
    public static final ConfigOption<Map<String, String>> JOB_CONFIGURATION_TO_MDC_KEYS =
            key("mdc.job-configuration-to-mdc-keys")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "Maps job configuration keys to MDC key names. "
                                    + "At job start, each listed configuration key is looked up; "
                                    + "if the value is present and non-blank it is emitted into MDC under the mapped name. "
                                    + "Keys absent or blank in the job configuration are skipped.");

    private MdcOptions() {}
}
