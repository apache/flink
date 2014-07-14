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

import java.util.ArrayList;
import java.util.HashMap;

import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class JoinTask extends UserTaskInvokable {
	private static final long serialVersionUID = 749913336259789039L;

	private HashMap<String, ArrayList<String>> gradeHashmap;
	private HashMap<String, ArrayList<Integer>> salaryHashmap;
	private StreamRecord outRecord = new StreamRecord(3);

	public JoinTask() {
		gradeHashmap = new HashMap<String, ArrayList<String>>();
		salaryHashmap = new HashMap<String, ArrayList<Integer>>();
	}

	@Override
	public void invoke(StreamRecord record) throws Exception {
		// TODO Auto-generated method stub
		String streamId = record.getString(0);
		String name = record.getString(1);
		if (streamId.equals("grade")) {
			if (salaryHashmap.containsKey(name)) {
				for (Integer salary : salaryHashmap.get(name)) {
					Tuple3<String, String, Integer> outputTuple = new Tuple3<String, String, Integer>(
							name, record.getString(2), salary);
					outRecord.addTuple(outputTuple);
				}
				emit(outRecord);
				outRecord.Clear();
			}
			if (!gradeHashmap.containsKey(name)) {
				gradeHashmap.put(name, new ArrayList<String>());
			}
			gradeHashmap.get(name).add(record.getString(2));
		} else {
			if (gradeHashmap.containsKey(name)) {
				for (String grade : gradeHashmap.get(name)) {
					Tuple3<String, String, Integer> outputTuple = new Tuple3<String, String, Integer>(
							name, grade, record.getInteger(2));
					outRecord.addTuple(outputTuple);
				}
				emit(outRecord);
				outRecord.Clear();
			}
			if (!salaryHashmap.containsKey(name)) {
				salaryHashmap.put(name, new ArrayList<Integer>());
			}
			salaryHashmap.get(name).add(record.getInteger(2));
		}
	}
}
