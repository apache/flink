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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Tests that validate the {@link CommandLineParser}.
 */
public class CommandLineParserTest {
	@Test
	public void testConfigDirArgs() throws Exception {
		String[] args = new String[]{"-configDir", "configDirPath1"};
		CommandLineParser parser = CommandLineParser.parse(args);
		String configDir = parser.getConfigDir("");
		assertEquals("configDirPath1", configDir);

		args = new String[]{"--configDir", "configDirPath2"};
		parser = CommandLineParser.parse(args);
		configDir = parser.getConfigDir("");
		assertEquals("configDirPath2", configDir);

		args = new String[]{"-c", "configDirPath3"};
		parser = CommandLineParser.parse(args);
		configDir = parser.getConfigDir("");
		assertEquals("configDirPath3", configDir);

		args = new String[]{"--c", "configDirPath4"};
		parser = CommandLineParser.parse(args);
		configDir = parser.getConfigDir("");
		assertEquals("configDirPath4", configDir);
	}

	@Test
	public void testInvalidConfig() throws Exception {
		String[] args = new String[]{"configDir", "configDirPath1"};
		CommandLineParser parser = CommandLineParser.parse(args);
		String configDir = parser.getConfigDir("");
		assertEquals("", configDir);

		args = new String[]{"---configDir", "configDirPath2"};
		parser = CommandLineParser.parse(args);
		configDir = parser.getConfigDir("");
		assertEquals("", configDir);

		args = new String[]{"c", "configDirPath3"};
		parser = CommandLineParser.parse(args);
		configDir = parser.getConfigDir("");
		assertEquals("", configDir);

		args = new String[]{"---c", "configDirPath4"};
		parser = CommandLineParser.parse(args);
		configDir = parser.getConfigDir("");
		assertEquals("", configDir);

		args = new String[]{"-f", "configDirFile"};
		parser = CommandLineParser.parse(args);
		configDir = parser.getConfigDir("");
		assertEquals("", configDir);
	}

	@Test
	public void testDynamicProperties() throws Exception {
		String[] args = new String[]{
			"-configDir", "configDirPath",
			"-D", "rpc.port=111",
			"-D", "rpc.address=localhost"};
		CommandLineParser parser = CommandLineParser.parse(args);
		assertEquals("configDirPath", parser.getConfigDir());

		Configuration dynamicProperties = parser.getDynamicProperties();
		assertEquals("111", dynamicProperties.getString("rpc.port", ""));
		assertEquals("localhost", dynamicProperties.getString("rpc.address", ""));
	}
}
