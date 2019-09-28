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

package org.apache.flink.table.client.gateway.utils;

import org.apache.flink.table.client.config.Environment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

/**
 * Utilities for reading an environment file.
 */
public final class EnvironmentFileUtil {

	private EnvironmentFileUtil() {
		// private
	}

	public static Environment parseUnmodified(String fileName) throws IOException {
		final URL url = EnvironmentFileUtil.class.getClassLoader().getResource(fileName);
		Objects.requireNonNull(url);
		return Environment.parse(url);
	}

	public static Environment parseModified(String fileName, Map<String, String> replaceVars) throws IOException {
		final URL url = EnvironmentFileUtil.class.getClassLoader().getResource(fileName);
		Objects.requireNonNull(url);
		String schema = FileUtils.readFileUtf8(new File(url.getFile()));

		for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
			schema = schema.replace(replaceVar.getKey(), replaceVar.getValue());
		}

		return Environment.parse(schema);
	}
}
