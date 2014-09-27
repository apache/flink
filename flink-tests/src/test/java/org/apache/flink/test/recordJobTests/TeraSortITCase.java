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


package org.apache.flink.test.recordJobTests;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

import org.apache.flink.api.common.Plan;
import org.apache.flink.test.recordJobs.sort.TeraSort;
import org.apache.flink.test.util.RecordAPITestBase;
import org.junit.Assert;


public class TeraSortITCase extends RecordAPITestBase {
	
	private static final String INPUT_DATA_FILE = "/testdata/terainput.txt";
	
	private String resultPath;

	public TeraSortITCase(){
		setTaskManagerNumSlots(DOP);
	}

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		String testDataPath = getClass().getResource(INPUT_DATA_FILE).toString();
		
		TeraSort ts = new TeraSort();
		return ts.getPlan(new Integer(DOP).toString(), testDataPath, resultPath);
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
