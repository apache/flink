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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.nephele.util.*;

/**
 * this class contains tests for the global configuration (parsing config dir information)
 * @author casp
 *
 */
public class GlobalConfigurationTest {
	
	/**
	 * this test creates several config files with values and cross-checks the resulting GlobalConf
	 */
	@Test
	public void testConfigurationXML(){
		
		//create temp folder for config files
		File tempdir = new File(CommonTestUtils.getTempDir() + File.separator + CommonTestUtils.getRandomDirectoryName() + File.separator);
		tempdir.mkdirs();
		System.out.println("created temp dir: " + tempdir.getAbsolutePath());
		
		
		File conffile1 = new File(tempdir.getAbsolutePath() + File.separator + CommonTestUtils.getRandomDirectoryName() + ".xml");
		File conffile2 = new File(tempdir.getAbsolutePath() + File.separator + CommonTestUtils.getRandomDirectoryName() + ".xml");
		
		try {
			PrintWriter pw1 = new PrintWriter(conffile1);
			PrintWriter pw2 = new PrintWriter(conffile2);
			
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
			
		} catch (FileNotFoundException e1) {
			fail("cannot write to temp directory " + CommonTestUtils.getTempDir());
			e1.printStackTrace();
		}

		GlobalConfiguration.loadConfiguration(tempdir.getAbsolutePath());
		
		Configuration co = GlobalConfiguration.getConfiguration();
		
		assertEquals(co.getString("mykey1", "null"), "myvalue1");
		assertEquals(co.getString("mykey2", "null"), "myvalue2");
		assertEquals(co.getString("mykey3", "null"), "myvalue3");
		assertEquals(co.getString("mykey4", "null"), "myvalue4");
		
		//test (wrong) string-to integer conversion. should return default value.
		assertEquals(co.getInteger("mykey1", 500), 500);
		assertEquals(co.getInteger("anything", 500), 500);
		assertEquals(co.getBoolean("notexistent", true), true);
		

		//XML parsind done, remove temp files
		conffile1.delete();
		conffile2.delete();
		tempdir.delete();
		
		//test include local configuration
		Configuration newconf = new Configuration();
		newconf.setInteger("mynewinteger", 1000);
		GlobalConfiguration.includeConfiguration(newconf);
		assertEquals(GlobalConfiguration.getInteger("mynewinteger", 0), 1000);
		
		//test local "sub" configuration
		String[] configparams = {"mykey1", "mykey2"};
		Configuration newconf2 = GlobalConfiguration.getConfiguration(configparams);
		
		assertEquals(newconf2.keySet().size(), 2);
		assertEquals(newconf2.getString("mykey1", "null"), "myvalue1");
		assertEquals(newconf2.getString("mykey2", "null"), "myvalue2");
		
	}

}
