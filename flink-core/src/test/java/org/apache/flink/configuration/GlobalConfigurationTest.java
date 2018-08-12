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

import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class contains tests for the global configuration (parsing configuration directory information).
 */
public class GlobalConfigurationTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testConfigurationYAML() {
		File tmpDir = tempFolder.getRoot();
		File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

		try {
			try (final PrintWriter pw = new PrintWriter(confFile)) {

				pw.println("###########################"); // should be skipped
				pw.println("# Some : comments : to skip"); // should be skipped
				pw.println("###########################"); // should be skipped
				pw.println("mykey1: myvalue1"); // OK, simple correct case
				pw.println("mykey2       : myvalue2"); // OK, whitespace before colon is correct
				pw.println("mykey3:myvalue3"); // SKIP, missing white space after colon
				pw.println(" some nonsense without colon and whitespace separator"); // SKIP
				pw.println(" :  "); // SKIP
				pw.println("   "); // SKIP (silently)
				pw.println(" "); // SKIP (silently)
				pw.println("mykey4: myvalue4# some comments"); // OK, skip comments only
				pw.println("   mykey5    :    myvalue5    "); // OK, trim unnecessary whitespace
				pw.println("mykey6: my: value6"); // OK, only use first ': ' as separator
				pw.println("mykey7: "); // SKIP, no value provided
				pw.println(": myvalue8"); // SKIP, no key provided

				pw.println("mykey9: myvalue9"); // OK
				pw.println("mykey9: myvalue10"); // OK, overwrite last value

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			Configuration conf = GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());

			// all distinct keys from confFile1 + confFile2 key
			assertEquals(6, conf.keySet().size());

			// keys 1, 2, 4, 5, 6, 7, 8 should be OK and match the expected values
			assertEquals("myvalue1", conf.getString("mykey1", null));
			assertEquals("myvalue2", conf.getString("mykey2", null));
			assertEquals("null", conf.getString("mykey3", "null"));
			assertEquals("myvalue4", conf.getString("mykey4", null));
			assertEquals("myvalue5", conf.getString("mykey5", null));
			assertEquals("my: value6", conf.getString("mykey6", null));
			assertEquals("null", conf.getString("mykey7", "null"));
			assertEquals("null", conf.getString("mykey8", "null"));
			assertEquals("myvalue10", conf.getString("mykey9", null));
		} finally {
			confFile.delete();
			tmpDir.delete();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailIfNull() {
		GlobalConfiguration.loadConfiguration(null);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testFailIfNotLoaded() {
		GlobalConfiguration.loadConfiguration("/some/path/" + UUID.randomUUID());
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testInvalidConfiguration() throws IOException {
		GlobalConfiguration.loadConfiguration(tempFolder.getRoot().getAbsolutePath());
	}

	@Test
	// We allow malformed YAML files
	public void testInvalidYamlFile() throws IOException {
		final File confFile = tempFolder.newFile(GlobalConfiguration.FLINK_CONF_FILENAME);

		try (PrintWriter pw = new PrintWriter(confFile)) {
			pw.append("invalid");
		}

		assertNotNull(GlobalConfiguration.loadConfiguration(tempFolder.getRoot().getAbsolutePath()));
	}

	@Test
	public void testHiddenKey() {
		assertTrue(GlobalConfiguration.isSensitive("password123"));
		assertTrue(GlobalConfiguration.isSensitive("123pasSword"));
		assertTrue(GlobalConfiguration.isSensitive("PasSword"));
		assertTrue(GlobalConfiguration.isSensitive("Secret"));
		assertFalse(GlobalConfiguration.isSensitive("Hello"));
	}

	@Test
	public void testCheckExistsMultiHSConfiguration() {
		File tmpDir = tempFolder.getRoot();
		File flinkConfFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);
		File hsConfFlie = new File(tmpDir, GlobalConfiguration.FLINK_HISTORY_SERVER_CONF_FILENAME);

		try {
			try (final PrintWriter pw = new PrintWriter(flinkConfFile)) {
				pw.println("historyserver.a: a");
				pw.println("historyserver.b: b");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			try (final PrintWriter pw = new PrintWriter(hsConfFlie)) {
				pw.println("historyserver.a: a");
				pw.println("historyserver.c: c");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			assertTrue(
				GlobalConfiguration.checkExistsMultiHSConfiguration(
					hsConfFlie.toPath(),
					flinkConfFile.toPath())
			);

			try (final PrintWriter pw = new PrintWriter(hsConfFlie)) {
				pw.println("jobmanager.a: a");
				pw.println("jobmanager.c: c");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			assertFalse(
				GlobalConfiguration.checkExistsMultiHSConfiguration(
					hsConfFlie.toPath(),
					flinkConfFile.toPath())
			);

			hsConfFlie.delete();

			assertFalse(
				GlobalConfiguration.checkExistsMultiHSConfiguration(
					hsConfFlie.toPath(),
					flinkConfFile.toPath())
			);
		} finally {
			flinkConfFile.delete();
			hsConfFlie.delete();
			tmpDir.delete();
		}
	}

	@Test
	public void testLoadHistoryServerConfiguration() {
		File tmpDir = tempFolder.getRoot();
		String configDir = tmpDir.getAbsolutePath();
		File flinkConfFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);
		File hsConfFlie = new File(tmpDir, GlobalConfiguration.FLINK_HISTORY_SERVER_CONF_FILENAME);

		try {
			try (final PrintWriter pw = new PrintWriter(flinkConfFile)) {
				pw.println("historyserver.a: a");
				pw.println("historyserver.b: b");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			try (final PrintWriter pw = new PrintWriter(hsConfFlie)) {
				pw.println("historyserver.a: a");
				pw.println("historyserver.c: c");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			//read from history server config file
			Configuration hsConfig = GlobalConfiguration.loadHistoryServerConfiguration(configDir);
			assertEquals(true, hsConfig.containsKey("historyserver.c"));
			assertEquals("c", hsConfig.getString("historyserver.c", null));
			assertEquals(false, hsConfig.containsKey("historyserver.b"));

			//combine dynamic properties
			Configuration dynamicConfig = new Configuration();
			dynamicConfig.setString("historyserver.d", "d");

			hsConfig = GlobalConfiguration.loadHistoryServerConfiguration(configDir, dynamicConfig);
			assertEquals("d", hsConfig.getString("historyserver.d", null));

			//delete history server config file, make it use old mode
			hsConfFlie.delete();
			hsConfig = GlobalConfiguration.loadHistoryServerConfiguration(configDir);
			assertEquals(true, hsConfig.containsKey("historyserver.b"));
			assertEquals("b", hsConfig.getString("historyserver.b", null));
			assertEquals(false, hsConfig.containsKey("historyserver.c"));

			hsConfig = GlobalConfiguration.loadHistoryServerConfiguration(configDir, dynamicConfig);
			assertEquals("d", hsConfig.getString("historyserver.d", null));
		} finally {
			flinkConfFile.delete();
			hsConfFlie.delete();
			tmpDir.delete();
		}
	}

}
