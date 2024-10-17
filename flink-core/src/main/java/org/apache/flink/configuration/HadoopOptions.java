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
import org.apache.flink.annotation.docs.Documentation;

/** The set of configuration options relating to hadoop settings. */
@PublicEvolving
public class HadoopOptions {
    @Documentation.Section({Documentation.Sections.ALL_TASK_MANAGER})
    public static final ConfigOption<Boolean> CALLER_CONTEXT_ENABLED =
            ConfigOptions.key("hdfs.caller-context.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("A config of whether hadoop caller context is enabled.");
}
