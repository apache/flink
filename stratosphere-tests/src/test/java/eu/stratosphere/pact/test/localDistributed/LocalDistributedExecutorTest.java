/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.test.localDistributed;

import java.io.File;
import java.io.FileWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.client.localDistributed.LocalDistributedExecutor;
import eu.stratosphere.pact.example.wordcount.WordCount;
import eu.stratosphere.pact.test.pactPrograms.WordCountITCase;


public class LocalDistributedExecutorTest {

	@Test
	public void testLocalDistributedExecutorWithWordCount() {
		try {
			// set up the files
			File inFile = File.createTempFile("wctext", ".in");
			File outFile = File.createTempFile("wctext", ".out");
			inFile.deleteOnExit();
			outFile.deleteOnExit();
			
			FileWriter fw = new FileWriter(inFile);
			fw.write(WordCountITCase.TEXT);
			fw.close();
			
			// run WordCount
			WordCount wc = new WordCount();
			LocalDistributedExecutor lde = new LocalDistributedExecutor();
			lde.startNephele(2);
			lde.run( wc.getPlan("4", "file://" + inFile.getAbsolutePath(), "file://" + outFile.getAbsolutePath()));
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Before
	public void cool() {
		System.err.println("Cooling");
		try {
			Thread.sleep(1000);
			System.gc();
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@After
	public void coolDownGC() {
		cool();
		System.exit(0);
	}
}
