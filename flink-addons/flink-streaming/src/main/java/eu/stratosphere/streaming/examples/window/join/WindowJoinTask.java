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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class WindowJoinTask extends UserTaskInvokable {

	class SalaryProgress {
		public SalaryProgress(int salary, long progress) {
			this.salary = salary;
			this.progress = progress;
		}

		int salary;
		long progress;
	}

	class GradeProgress {
		public GradeProgress(String grade, long progress) {
			this.grade = grade;
			this.progress = progress;
		}

		String grade;
		long progress;
	}

	private static final long serialVersionUID = 749913336259789039L;
	private int windowSize = 100;
	private HashMap<String, LinkedList<GradeProgress>> gradeHashmap;
	private HashMap<String, LinkedList<SalaryProgress>> salaryHashmap;
	private StreamRecord outRecord = new StreamRecord(3);

	public WindowJoinTask() {
		gradeHashmap = new HashMap<String, LinkedList<GradeProgress>>();
		salaryHashmap = new HashMap<String, LinkedList<SalaryProgress>>();
	}

	@Override
	public void invoke(StreamRecord record) throws Exception {
		// TODO Auto-generated method stub
		String streamId = record.getString(0);
		String name = record.getString(1);
		long progress = record.getLong(3);
		if (streamId.equals("grade")) {
			if (salaryHashmap.containsKey(name)) {
				Iterator<SalaryProgress> iterator = salaryHashmap.get(name)
						.iterator();
				while (iterator.hasNext()) {
					SalaryProgress entry = iterator.next();
					if (progress - entry.progress > windowSize) {
						iterator.remove();
					} else {
						Tuple3<String, String, Integer> outputTuple = new Tuple3<String, String, Integer>(
								name, record.getString(2), entry.salary);
						outRecord.addTuple(outputTuple);
					}
				}
				if (outRecord.getNumOfTuples() != 0) {
					emit(outRecord);
				}
				outRecord.Clear();
			}
			if (!gradeHashmap.containsKey(name)) {
				gradeHashmap.put(name, new LinkedList<GradeProgress>());
			}
			gradeHashmap.get(name).add(
					new GradeProgress(record.getString(2), progress));
		} else {
			if (gradeHashmap.containsKey(name)) {
				Iterator<GradeProgress> iterator = gradeHashmap.get(name)
						.iterator();
				while (iterator.hasNext()) {
					GradeProgress entry = iterator.next();
					if (progress - entry.progress > windowSize) {
						iterator.remove();
					} else {
						Tuple3<String, String, Integer> outputTuple = new Tuple3<String, String, Integer>(
								name, entry.grade, record.getInteger(2));
						outRecord.addTuple(outputTuple);
					}
				}
				if (outRecord.getNumOfTuples() != 0) {
					emit(outRecord);
				}
				outRecord.Clear();
			}
			if (!salaryHashmap.containsKey(name)) {
				salaryHashmap.put(name, new LinkedList<SalaryProgress>());
			}
			salaryHashmap.get(name).add(
					new SalaryProgress(record.getInteger(2), progress));
		}
	}
}
