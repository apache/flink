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
package eu.stratosphere.test.clients.examples;

import java.io.File;
import java.io.FileWriter;

import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.test.recordJobs.wordcount.WordCount;
import eu.stratosphere.test.testdata.WordCountData;


public class LocalExecutorITCase {

	private static final int DOP = 4;

	@Test
	public void testLocalExecutorWithWordCount() {
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

			LocalExecutor executor = new LocalExecutor();
			LocalExecutor.setLoggingLevel(Level.WARN);
			executor.setDefaultOverwriteFiles(true);
			executor.setTaskManagerNumSlots(DOP);
			executor.start();
			
			executor.executePlan(wc.getPlan(new Integer(DOP).toString(), inFile.toURI().toString(),
					outFile.toURI().toString()));
			executor.stop();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		
	}
}
