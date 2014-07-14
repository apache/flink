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

package eu.stratosphere.streaming.examples.window.join;

import java.util.Random;

import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class WindowJoinSourceTwo extends UserSourceInvokable {

	private static final long serialVersionUID = -5897483980082089771L;

	private String[] names = { "tom", "jerry", "alice", "bob", "john", "grace",
			"sasa", "lawrance", "andrew", "jean", "richard", "smith", "gorge",
			"black", "peter" };
	private Random rand = new Random();
	private StreamRecord outRecord = new StreamRecord(
			new Tuple4<String, String, String, Long>());
	private long progress = 0L;

	@Override
	public void invoke() throws Exception {
		while (true) {
			outRecord.setString(0, "grade");
			outRecord.setString(1, names[rand.nextInt(names.length)]);
			outRecord.setString(2, String.valueOf((char)(rand.nextInt(26)+'A')));
			outRecord.setLong(3, progress);
			emit(outRecord);
			progress+=1;
		}
	}
}
