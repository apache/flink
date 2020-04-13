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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.List;

/**
 * Validator for {@link FileSystem}.
 */
@PublicEvolving
public class FileSystemValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE = "filesystem";
	public static final String CONNECTOR_PATH = "connector.path";
	public static final String NUM_FILE = "num.file";
	public static final String WRITE_MODE = "write.mode";
	public static final String OVERWRITE = "OVERWRITE";
	public static final String NO_OVERWRITE = "NO_OVERWRITE";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE, false);
		properties.validateString(CONNECTOR_PATH, false, 1);
		properties.validateInt(NUM_FILE, true);
		List<String> writeModeList = Arrays.asList(OVERWRITE, NO_OVERWRITE);
		properties.validateEnumValues(WRITE_MODE, true, writeModeList);
	}
}
