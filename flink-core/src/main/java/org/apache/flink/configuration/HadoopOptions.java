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

/** The set of configuration options relating to hadoop settings. */
@PublicEvolving
public class HadoopOptions {

    public static final ConfigOption<String> CALLER_CONTEXT_APP_ID =
            ConfigOptions.key("execution.caller-context-app-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A text means the hadoop caller context app id.");
}
