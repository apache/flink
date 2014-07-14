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

package eu.stratosphere.streaming.examples.join;

import java.util.Random;

import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class JoinSourceTwo extends UserSourceInvokable {

	private static final long serialVersionUID = -5897483980082089771L;

	private String[] names = { "tom", "jerry", "alice", "bob", "john", "grace",
			"sasa", "lawrance", "andrew", "jean", "richard", "smith", "gorge",
			"black", "peter" };
	private Random rand = new Random();
	private StreamRecord outRecord = new StreamRecord(
			new Tuple3<String, String, String>());

	@Override
	public void invoke() throws Exception {
		while (true) {
			outRecord.setString(0, "grade");
			outRecord.setString(1, names[rand.nextInt(names.length)]);
			outRecord.setString(2, String.valueOf((char)(rand.nextInt(26)+'A')));
			emit(outRecord);
		}
	}
}
