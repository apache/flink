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

package org.apache.flink.docs.configuration;

import org.apache.flink.configuration.ConfigConstants;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * A small utility that collects all config keys that are not described in
 * the configuration reference documentation.
 */
public class ConfigDocsCompletenessChecker {

	public static void main(String[] args) throws Exception {

		String configFileContents = FileUtils.readFileToString(new File("docs/setup/config.md"));
		Field[] fields = ConfigConstants.class.getFields();

		for (Field field : fields) {
			if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(String.class) && !field.getName().startsWith("DEFAULT")) {
				Object val = field.get(null);
				if (!configFileContents.contains((String) val)) {
					System.out.println("++++ " + val + " is not mentioned in the configuration file!!!");
				}
			}
		}
	}
}
