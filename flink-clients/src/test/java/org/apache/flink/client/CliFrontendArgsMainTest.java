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
package org.apache.flink.client;


import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.MainOptions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.net.MalformedURLException;

import static org.apache.flink.client.CliFrontendTestUtils.getTestJarPath;
import static org.junit.Assert.assertEquals;

public class CliFrontendArgsMainTest {


	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@Test
	public void simpleParse() throws FileNotFoundException, MalformedURLException, CliArgsException {
			String[] parameters = {"--configDir", "expectedConfigDirectory", "run", getTestJarPath()};
			MainOptions options = CliFrontendParser.parseMainCommand(parameters);
			assertEquals("expectedConfigDirectory", options.getConfigDir());
	}

	@Test
	public void dropAfterAction() throws FileNotFoundException, MalformedURLException, CliArgsException {
		String[] parameters = {"--configDir", "expectedConfigDirectory", "run", getTestJarPath(), "--configDir", "notExpected"};
		MainOptions options = CliFrontendParser.parseMainCommand(parameters);
		assertEquals("expectedConfigDirectory", options.getConfigDir());
		assertEquals(options.getOptions().length, 1);
	}
}
