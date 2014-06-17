/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.test.exampleJavaPrograms;

import java.io.File;
import java.io.FilenameFilter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.client.program.PackagedProgram;

public class JsonValidationCase {

	private static File[] jobFiles;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		File targetFolder = new File("../stratosphere-examples/stratosphere-java-examples/target");
		jobFiles = targetFolder.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		        return name.toLowerCase().endsWith(".jar");
		    }
		});
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		for (File f : jobFiles) {
			checkJsonForFile(f);
		}
	}
	
	private static void checkJsonForFile(File f) {
		boolean exception = false;
		try {
			PackagedProgram prog = new PackagedProgram(f);
			JsonFactory fact = new JsonFactory();
			JsonParser parser = fact.createJsonParser(prog.getPreviewPlan());
			while (parser.nextToken() != null) {
				
			}
		} catch (JsonParseException e) {
			Assert.fail("Json of following job was not valid: " + f.getName());
		} catch (Exception e) {
			//ok, because it is possible that Job needs arguments -> this test would fail
			exception = true;
		}
		if (!exception)
			System.err.println("Valid job: " + f.getName());
	}
	
	@SuppressWarnings("unused")
	private static void printJobFiles() {
		for (File f : jobFiles) {
			System.out.println(f.getAbsolutePath());
		}
	}
}
