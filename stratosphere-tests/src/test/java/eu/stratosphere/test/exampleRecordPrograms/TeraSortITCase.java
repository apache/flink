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

package eu.stratosphere.test.exampleRecordPrograms;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

import org.junit.Assert;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.example.java.record.sort.TeraSort;
import eu.stratosphere.test.util.TestBase2;


public class TeraSortITCase extends TestBase2 {
	
	private static final String INPUT_DATA_FILE = "/testdata/terainput.txt";
	
	private String resultPath;
	

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		String testDataPath = getClass().getResource(INPUT_DATA_FILE).toString();
		
		TeraSort ts = new TeraSort();
		return ts.getPlan("4", testDataPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		final byte[] line = new byte[100];
		final byte[] previous = new byte[10];
		for (int i = 0; i < previous.length; i++) {
			previous[i] = -128;
		}
		
		File parent = new File(new URI(resultPath).getPath());
		int num = 1;
		while (true) {
			File next = new File(parent, String.valueOf(num));
			if (!next.exists()) {
				break;
			}
			FileInputStream inStream = new FileInputStream(next);
			int read;
			while ((read = inStream.read(line)) == 100) {
				// check against the previous
				for (int i = 0; i < previous.length; i++) {
					if (line[i] > previous[i]) {
						break;
					} else if (line[i] < previous[i]) {
						Assert.fail("Next record is smaller than previous record.");
					}
				}
				
				System.arraycopy(line, 0, previous, 0, 10);
			}
			
			if (read != -1) {
				Assert.fail("Inclomplete last record in result file.");
			}
			inStream.close();
			
			num++;
		}
		
		if (num == 1) {
			Assert.fail("Empty result, nothing checked for Job!");
		}
	}
}
