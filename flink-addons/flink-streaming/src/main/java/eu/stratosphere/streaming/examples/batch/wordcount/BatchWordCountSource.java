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

package eu.stratosphere.streaming.examples.batch.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class BatchWordCountSource extends UserSourceInvokable {

	private BufferedReader br = null;
	private String line = "";
	private StreamRecord outRecord = new StreamRecord(new Tuple2<String, Long>());

	private final static int BATCH_SIZE = 20;

	private Long timestamp = 0L;

	public BatchWordCountSource() {
		try {
			br = new BufferedReader(new FileReader("src/test/resources/testdata/hamlet.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void invoke() throws Exception {
		timestamp = 0L;
		outRecord = new StreamRecord(2);

		line = br.readLine().replaceAll("[\\-\\+\\.\\^:,]", "");

		while (line != null) {
			if (line != "") {

				outRecord.addTuple(new Tuple2<String, Long>(line, timestamp));
				timestamp++;
				if (timestamp % BATCH_SIZE == 0) {
					emit(outRecord);
					outRecord = new StreamRecord(2);
				}
			}

			line = br.readLine();

		}
	}

}