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
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.PerformanceCounter;
import eu.stratosphere.streaming.util.PerformanceTimer;

public class WordCountDummySource2 extends UserSourceInvokable {

	StreamRecord record = new StreamRecord(new Tuple1<String>());
	PerformanceCounter pCounter = new PerformanceCounter("SourceEmitCounter", 1000, 1000);
	PerformanceTimer pTimer = new PerformanceTimer("SourceEmitTimer", 1000, 1000, true);

	public WordCountDummySource2() {
	}

	@Override
	public void invoke() throws Exception {

		for (int i = 0; i < 1000000; i++) {

			if (i % 2 == 0) {
				record.setString(0, "Gyula Bela");
			} else {
				record.setString(0, "Gabor Frank");
			}
			pTimer.startTimer();
			emit(record);
			pTimer.stopTimer();
			pCounter.count();
		}
	}

	@Override
	public String getResult() {
		pCounter.writeCSV("/home/strato/log/counter/Source" + channelID);
		pTimer.writeCSV("/home/strato/log/timer/Source" + channelID);

		return "";
	}
}
