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

package org.apache.flink.table.runtime.arrow.sources;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Table options for the {@link ArrowTableSource}. */
public class ArrowTableSourceOptions {

    public static final ConfigOption<String> DATA =
            ConfigOptions.key("data")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "This is the data serialized by Arrow with a byte two-dimensional array. "
                                    + "Note: The byte two-dimensional array is converted into a string using base64.");
}
