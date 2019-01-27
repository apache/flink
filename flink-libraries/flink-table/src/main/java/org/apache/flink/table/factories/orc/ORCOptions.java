/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.factories.orc;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/** ORC options. **/
public class ORCOptions {
	public static final ConfigOption<String> FILE_PATH = key("filePath".toLowerCase())
		.noDefaultValue();

	public static final ConfigOption<Boolean> ENUMERATE_NESTED_FILES = key("enumerateNestedFiles".toLowerCase())
		.defaultValue(true);

	public static final ConfigOption<String> WRITE_MODE = key("writeMode".toLowerCase())
		.defaultValue("None");

	public static final ConfigOption<String> COMPRESSION_CODEC_NAME = key("compressionCodecName".toLowerCase())
		.defaultValue("SNAPPY");

	public static final List<String> SUPPORTED_KEYS = Arrays.asList(FILE_PATH.key(), ENUMERATE_NESTED_FILES.key(),
		WRITE_MODE.key(), COMPRESSION_CODEC_NAME.key());

	public static final String PARAMS_HELP_MSG = String.format("required params:%s", FILE_PATH);
}
