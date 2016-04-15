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

package org.apache.flink.testutils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.flink.configuration.GlobalConfiguration;

/**
 * Utility class to help test the Flink configuration.
 */
public final class TestConfigUtils {
	
	public static void loadGlobalConf(String[] keys, String[] values) throws IOException {
		loadGlobalConf(getConfAsString(keys, values));
	}
	
	public static void loadGlobalConf(String contents) throws IOException {
		final File tempDir = new File(System.getProperty("java.io.tmpdir"));
		File confDir = null;
		do {
			confDir = new File(tempDir, TestFileUtils.randomFileName());
		} while (confDir.exists());
		
		try {
			confDir.mkdirs();
			final File confFile = new File(confDir, "tempConfig.xml");
		
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(confFile));
				try {
					writer.write(contents);
				} finally {
					writer.close();
				}
				GlobalConfiguration.loadConfiguration(confDir.getAbsolutePath());
			} finally {
				confFile.delete();
			}
		}
		finally {
			confDir.delete();
		}
	}
	
	public static String getConfAsString(String[] keys, String[] values) {
		if (keys == null || values == null || keys.length != values.length) {
			throw new IllegalArgumentException();
		}
		
		StringBuilder bld = new StringBuilder();
		bld.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<configuration>\n");
		
		for (int i = 0; i < keys.length; i++) {
			bld.append("<property>\n<key>").append(keys[i]).append("</key>\n");
			bld.append("<value>").append(values[i]).append("</value>\n</property>\n");
		}
		bld.append("</configuration>\n");
		return bld.toString();
	}

	// ------------------------------------------------------------------------
	
	private TestConfigUtils() {}

}
