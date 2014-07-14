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

package eu.stratosphere.streaming.examples.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.PerformanceCounter;
import eu.stratosphere.streaming.util.PerformanceTimer;

public class WordCountSourceSplitter extends UserSourceInvokable {

	private BufferedReader br = null;
	private String line = new String();
	private StreamRecord outRecord = new StreamRecord(new Tuple1<String>());

	PerformanceCounter pCounter = new PerformanceCounter("SourceEmitCounter", 1000, 1000);
	PerformanceTimer pTimer = new PerformanceTimer("SourceEmitTimer", 1000, 1000, true);

	@Override
	public void invoke() throws Exception {

		for (int i = 0; i < 10; i++) {
			try {
				br = new BufferedReader(new FileReader(
						"/home/strato/stratosphere-distrib/resources/hamlet.txt"));

				line = br.readLine().replaceAll("[\\-\\+\\.\\^:,]", "");
				while (line != null) {
					if (line != "") {
						for (String word : line.split(" ")) {
							outRecord.setString(0, word);
							pTimer.startTimer();
							emit(outRecord);
							pTimer.stopTimer();
							pCounter.count();
						}
					}
					line = br.readLine();
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public String getResult() {
		pCounter.writeCSV("/home/strato/stratosphere-distrib/log/counter/Source" + channelID);
		pTimer.writeCSV("/home/strato/stratosphere-distrib/log/timer/Source" + channelID);

		return "";
	}
}