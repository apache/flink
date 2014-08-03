/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.examples.window.join;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class WindowJoinTask extends
		RichFlatMapFunction<Tuple4<String, String, Integer, Long>, Tuple3<String, Integer, Integer>> {

	class SalaryProgress {
		public SalaryProgress(Integer salary, Long progress) {
			this.salary = salary;
			this.progress = progress;
		}

		Integer salary;
		Long progress;
	}

	class GradeProgress {
		public GradeProgress(Integer grade, Long progress) {
			this.grade = grade;
			this.progress = progress;
		}

		Integer grade;
		Long progress;
	}

	private static final long serialVersionUID = 749913336259789039L;
	private int windowSize = 100;
	private HashMap<String, LinkedList<GradeProgress>> gradeHashmap;
	private HashMap<String, LinkedList<SalaryProgress>> salaryHashmap;

	public WindowJoinTask() {
		gradeHashmap = new HashMap<String, LinkedList<GradeProgress>>();
		salaryHashmap = new HashMap<String, LinkedList<SalaryProgress>>();
	}

	@Override
	public void flatMap(Tuple4<String, String, Integer, Long> value,
			Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
		String streamId = value.f0;
		String name = value.f1;
		Long progress = value.f3;
		
		// Joins the input value with the already known values on a given interval. If it is a grade
		// then with the salaries, if it is a salary then with the grades. Also
		// stores the new element.
		if (streamId.equals("grade")) {
			if (salaryHashmap.containsKey(name)) {
				Iterator<SalaryProgress> iterator = salaryHashmap.get(name).iterator();
				while (iterator.hasNext()) {
					SalaryProgress entry = iterator.next();
					if (progress - entry.progress > windowSize) {
						iterator.remove();
					} else {
						Tuple3<String, Integer, Integer> outputTuple = new Tuple3<String, Integer, Integer>(
								name, value.f2, entry.salary);
						out.collect(outputTuple);
					}
				}
				if (!gradeHashmap.containsKey(name)) {
					gradeHashmap.put(name, new LinkedList<GradeProgress>());
				}
				gradeHashmap.get(name).add(new GradeProgress(value.f2, progress));
			} else {
				if (gradeHashmap.containsKey(name)) {
					Iterator<GradeProgress> iterator = gradeHashmap.get(name).iterator();
					while (iterator.hasNext()) {
						GradeProgress entry = iterator.next();
						if (progress - entry.progress > windowSize) {
							iterator.remove();
						} else {
							Tuple3<String, Integer, Integer> outputTuple = new Tuple3<String, Integer, Integer>(
									name, entry.grade, value.f2);
							out.collect(outputTuple);

						}
					}
				}
				if (!salaryHashmap.containsKey(name)) {
					salaryHashmap.put(name, new LinkedList<SalaryProgress>());
				}
				salaryHashmap.get(name).add(new SalaryProgress(value.f2, progress));
			}

		}
	}
}
