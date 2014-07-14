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

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.PerformanceCounter;
import eu.stratosphere.streaming.util.PerformanceTimer;

public class WordCountSplitter extends UserTaskInvokable {

	private String[] words = new String[] {};
	private StreamRecord outputRecord = new StreamRecord(new Tuple1<String>());
	PerformanceCounter pCounter = new PerformanceCounter("SplitterEmitCounter", 1000, 1000);
	PerformanceTimer pTimer = new PerformanceTimer("SplitterEmitTimer", 1000, 1000, true);

	@Override
	public void invoke(StreamRecord record) throws Exception {

		words = record.getString(0).split(" ");
		for (String word : words) {
			outputRecord.setString(0, word);
			pTimer.startTimer();
			emit(outputRecord);
			pTimer.stopTimer();
			pCounter.count();
		}
	}

	@Override
	public String getResult() {
		pCounter.writeCSV("/home/strato/stratosphere-distrib/log/counter/Splitter" + channelID);
		pTimer.writeCSV("/home/strato/stratosphere-distrib/log/timer/Splitter" + channelID);
		return "";
	}
}