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

package org.apache.flink.docs.configuration.data;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Collection of test {@link ConfigOptions ConfigOptions}.
 */
public class TestCommonOptions {

	@Documentation.CommonOption
	public static ConfigOption<Integer> firstOption = ConfigOptions
		.key("first.option.a")
		.defaultValue(2)
		.withDescription("This is example description for the first option.");

	public static ConfigOption<String> secondOption = ConfigOptions
		.key("second.option.a")
		.noDefaultValue()
		.withDescription("This is long example description for the second option.");
}
