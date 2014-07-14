/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.test.window.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class WindowWordCountSource extends UserSourceInvokable {
	private BufferedReader br = null;
	private String line;
	private long timestamp;
	private Record hamletRecord = null;

	public WindowWordCountSource() {
		try {
			br = new BufferedReader(
					new FileReader(
							"src/main/java/eu/stratosphere/streaming/test/window/wordcount/hamlet.txt"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void invoke() throws Exception {
		line = br.readLine().replaceAll("[\\-\\+\\.\\^:,]", "");
		timestamp = 0;
		// while (line != null) {
		for (int i = 0; i < 2; ++i) {
			if (line != "") {
				hamletRecord = new Record(new StringValue(line), new LongValue(timestamp));
				System.out.println("========line number: " + timestamp + ", "
						+ line + "==========");
				emit(hamletRecord);
				line = br.readLine();
				++timestamp;
			}
		}
	}
}
