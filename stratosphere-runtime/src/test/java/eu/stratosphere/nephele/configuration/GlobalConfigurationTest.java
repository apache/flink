/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
import java.lang.reflect.Field;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class contains tests for the global configuration (parsing configuration directory information).
 */
public class GlobalConfigurationTest {

    @Before
    public void resetSingleton() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    	// reset GlobalConfiguration between tests 
       Field instance = GlobalConfiguration.class.getDeclaredField("configuration");
       instance.setAccessible(true);
       instance.set(null, null);
    }
	
	@Test
	public void testConfigurationMixed() {
		File tmpDir = getTmpDir();
		File confFile1 = createRandomFile(tmpDir, ".yaml");
		File confFile2 = createRandomFile(tmpDir, ".xml");

		try {
			try {
				PrintWriter pw1 = new PrintWriter(confFile1);
				PrintWriter pw2 = new PrintWriter(confFile2);
				
				pw1.println("mykey1: myvalue1_YAML");
				pw1.println("mykey2: myvalue2");
				
				pw2.println("<configuration>");
				pw2.println("<property><key>mykey1</key><value>myvalue1_XML</value></property>");
				pw2.println("<property><key>mykey3</key><value>myvalue3</value></property>");
				pw2.println("</configuration>");
				
				pw1.close();
				pw2.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
			GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());
			Configuration conf = GlobalConfiguration.getConfiguration();
			
			// all distinct keys from confFile1 + confFile2 + 'config.dir' key
			assertEquals(3 + 1, conf.keySet().size());
			
			// keys 1, 2, 3 should be OK and match the expected values
			// => configuration keys from YAML should overwrite keys from XML
			assertEquals("myvalue1_YAML", conf.getString("mykey1", null));
			assertEquals("myvalue2", conf.getString("mykey2", null));
			assertEquals("myvalue3", conf.getString("mykey3", null));
		} finally {
			confFile1.delete();
			confFile2.delete();
			tmpDir.delete();
		}
	}
	
	@Test
	public void testConfigurationYAML() {
		File tmpDir = getTmpDir();
		File confFile1 = createRandomFile(tmpDir, ".yaml");
		File confFile2 = createRandomFile(tmpDir, ".yaml");

		try {
			try {
				PrintWriter pw1 = new PrintWriter(confFile1);
				PrintWriter pw2 = new PrintWriter(confFile2);

				pw1.println("###########################"); // should be skipped
				pw1.println("# Some : comments : to skip"); // should be skipped
				pw1.println("###########################"); // should be skipped
				pw1.println("mykey1: myvalue1"); // OK, simple correct case
				pw1.println("mykey2       : myvalue2"); // OK, whitespace before colon is correct
				pw1.println("mykey3:myvalue3"); // SKIP, missing white space after colon
				pw1.println(" some nonsense without colon and whitespace separator"); // SKIP
				pw1.println(" :  "); // SKIP
				pw1.println("   "); // SKIP
				pw1.println("mykey4: myvalue4# some comments"); // OK, skip comments only
				pw1.println("   mykey5    :    myvalue5    "); // OK, trim unnecessary whitespace
				pw1.println("mykey6: my: value6"); // OK, only use first ': ' as separator
				pw1.println("mykey7: "); // SKIP, no value provided
				pw1.println(": myvalue8"); // SKIP, no key provided

				pw2.println("mykey9: myvalue9"); // OK
				pw2.println("mykey9: myvalue10"); // OK, overwrite last value

				pw1.close();
				pw2.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());
			Configuration conf = GlobalConfiguration.getConfiguration();

			// all distinct keys from confFile1 + confFile2 + 'config.dir' key
			assertEquals(6 + 1, conf.keySet().size());

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
			confFile1.delete();
			confFile2.delete();
			tmpDir.delete();
		}
	}

	/**
	 * This test creates several configuration files with values and cross-checks the resulting
	 * {@link GlobalConfiguration} object.
	 */
	@Test
	public void testConfigurationXML() {

		// Create temporary directory for configuration files
		final File tmpDir = getTmpDir();
		final File confFile1 = createRandomFile(tmpDir, ".xml");
		final File confFile2 = createRandomFile(tmpDir, ".xml");

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

			GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());

			final Configuration co = GlobalConfiguration.getConfiguration();

			assertEquals(co.getString("mykey1", "null"), "myvalue1");
			assertEquals(co.getString("mykey2", "null"), "myvalue2");
			assertEquals(co.getString("mykey3", "null"), "myvalue3");
			assertEquals(co.getString("mykey4", "null"), "myvalue4");

			// // Test (wrong) string-to integer conversion. should return default value.
			// semantics are changed to throw an exception upon invalid parsing!
			// assertEquals(co.getInteger("mykey1", 500), 500);
			// assertEquals(co.getInteger("anything", 500), 500);
			// assertEquals(co.getBoolean("notexistent", true), true);

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
			tmpDir.delete();
		}
	}

	private File getTmpDir() {
		File tmpDir = new File(CommonTestUtils.getTempDir() + File.separator
			+ CommonTestUtils.getRandomDirectoryName() + File.separator);
		tmpDir.mkdirs();

		return tmpDir;
	}

	private File createRandomFile(File path, String suffix) {
		return new File(path.getAbsolutePath() + File.separator + CommonTestUtils.getRandomDirectoryName() + suffix);
	}

}
