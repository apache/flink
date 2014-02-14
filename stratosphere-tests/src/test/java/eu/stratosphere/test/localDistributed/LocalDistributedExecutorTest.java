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
package eu.stratosphere.test.localDistributed;

import eu.stratosphere.client.localDistributed.LocalDistributedExecutor;
import eu.stratosphere.example.java.record.wordcount.WordCount;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.util.OperatingSystem;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

public class LocalDistributedExecutorTest {

	@Test
	public void testLocalDistributedExecutorWithWordCount() {

		LocalDistributedExecutor lde = new LocalDistributedExecutor();

		try {
			// set up the files
			File inFile = File.createTempFile("wctext", ".in");
			File outFile = File.createTempFile("wctext", ".out");
			inFile.deleteOnExit();
			outFile.deleteOnExit();
			
			FileWriter fw = new FileWriter(inFile);
			fw.write(WordCountData.TEXT);
			fw.close();
			
			// run WordCount
			WordCount wc = new WordCount();

			lde.start(2);
			lde.run(wc.getPlan("4", inFile.toURI().toString(), outFile.toURI().toString()));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		} finally {
			try {
				lde.stop();
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail(e.getMessage());
			}
		}
	}
}
