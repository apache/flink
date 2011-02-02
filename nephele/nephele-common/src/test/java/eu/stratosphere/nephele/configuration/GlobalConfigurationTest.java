/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class contains tests for the global configuration (parsing configuration directory information).
 * 
 * @author casp
 */
public class GlobalConfigurationTest {

	/**
	 * This test creates several configuration files with values and cross-checks the resulting
	 * {@link GlobalConfiguration} object.
	 */
	@Test
	public void testConfigurationXML() {

		// Create temporary directory for configuration files
		final File tempdir = new File(CommonTestUtils.getTempDir() + File.separator
			+ CommonTestUtils.getRandomDirectoryName() + File.separator);
		tempdir.mkdirs();

		final File confFile1 = new File(tempdir.getAbsolutePath() + File.separator
			+ CommonTestUtils.getRandomDirectoryName() + ".xml");
		final File confFile2 = new File(tempdir.getAbsolutePath() + File.separator
			+ CommonTestUtils.getRandomDirectoryName() + ".xml");

		try {
			try {
				final PrintWriter pw1 = new PrintWriter(confFile1);
				final PrintWriter pw2 = new PrintWriter(confFile2);

				pw1.append("<configuration>");
				pw2.append("<configuration>");

				pw1.append("<property><key>mykey1</key><value>myvalue1</value></property>");
				pw1.append("<property></property>");
				pw1.append("<property><key></key><value></value></property>");
				pw1.append("<property><key>hello</key><value></value></property>");
				pw1.append("<property><key>mykey2</key><value>myvalue2</value></property>");
				pw2.append("<property><key>mykey3</key><value>myvalue3</value></property>");
				pw2.append("<property><key>mykey4</key><value>myvalue4</value></property>");

				pw1.append("</configuration>");
				pw2.append("</configuration>");
				pw1.close();
				pw2.close();
			} catch (FileNotFoundException e) {
				fail(e.getMessage());
			}

			GlobalConfiguration.loadConfiguration(tempdir.getAbsolutePath());

			final Configuration co = GlobalConfiguration.getConfiguration();

			assertEquals(co.getString("mykey1", "null"), "myvalue1");
			assertEquals(co.getString("mykey2", "null"), "myvalue2");
			assertEquals(co.getString("mykey3", "null"), "myvalue3");
			assertEquals(co.getString("mykey4", "null"), "myvalue4");

			// Test (wrong) string-to integer conversion. should return default value.
			assertEquals(co.getInteger("mykey1", 500), 500);
			assertEquals(co.getInteger("anything", 500), 500);
			assertEquals(co.getBoolean("notexistent", true), true);

			// Test include local configuration
			final Configuration newconf = new Configuration();
			newconf.setInteger("mynewinteger", 1000);
			GlobalConfiguration.includeConfiguration(newconf);
			assertEquals(GlobalConfiguration.getInteger("mynewinteger", 0), 1000);

			// Test local "sub" configuration
			final String[] configparams = { "mykey1", "mykey2" };
			Configuration newconf2 = GlobalConfiguration.getConfiguration(configparams);

			assertEquals(newconf2.keySet().size(), 2);
			assertEquals(newconf2.getString("mykey1", "null"), "myvalue1");
			assertEquals(newconf2.getString("mykey2", "null"), "myvalue2");
		} finally {

			// Remove temporary files
			confFile1.delete();
			confFile2.delete();
			tempdir.delete();
		}

	}

}
