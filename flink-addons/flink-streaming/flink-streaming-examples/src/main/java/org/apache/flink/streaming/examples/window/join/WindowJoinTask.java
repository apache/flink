/*
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
 */

package org.apache.flink.streaming.examples.window.join;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.function.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class WindowJoinTask extends
		RichCoFlatMapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Integer>> {

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
	private String name;
	private Long progress;
	
	public WindowJoinTask() {
		gradeHashmap = new HashMap<String, LinkedList<GradeProgress>>();
		salaryHashmap = new HashMap<String, LinkedList<SalaryProgress>>();
		name = new String();
		progress = 0L;
	}

	Tuple3<String, Integer, Integer> outputTuple = new Tuple3<String, Integer, Integer>();
	
	// Joins the input value (grade) with the already known values (salaries) on
	// a given interval.
	// Also stores the new element.
	@Override
	public void flatMap1(Tuple3<String, Integer, Long> value,
			Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
		name = value.f0;
		progress = value.f2;

		outputTuple.f0 = name;
		outputTuple.f1 = value.f1;
		
		if (salaryHashmap.containsKey(name)) {
			Iterator<SalaryProgress> iterator = salaryHashmap.get(name).iterator();
			while (iterator.hasNext()) {
				SalaryProgress entry = iterator.next();
				if (progress - entry.progress > windowSize) {
					iterator.remove(); 
				} else {
					outputTuple.f2 = entry.salary;
					out.collect(outputTuple);
				}
			}
		}
		if (!gradeHashmap.containsKey(name)) {
			gradeHashmap.put(name, new LinkedList<GradeProgress>());
		}
		gradeHashmap.get(name).add(new GradeProgress(value.f1, progress));
	}

	// Joins the input value (salary) with the already known values (grades) on
	// a given interval.
	// Also stores the new element.
	@Override
	public void flatMap2(Tuple3<String, Integer, Long> value,
			Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
		name = value.f0;
		progress = value.f2;

		outputTuple.f0 = name;
		outputTuple.f2 = value.f1;
		
		if (gradeHashmap.containsKey(name)) {
			Iterator<GradeProgress> iterator = gradeHashmap.get(name).iterator();
			while (iterator.hasNext()) {
				GradeProgress entry = iterator.next();
				if (progress - entry.progress > windowSize) {
					iterator.remove();
				} else {
					outputTuple.f1 = entry.grade;
					out.collect(outputTuple);
				}
			}
		}
		if (!salaryHashmap.containsKey(name)) {
			salaryHashmap.put(name, new LinkedList<SalaryProgress>());
		}
		salaryHashmap.get(name).add(new SalaryProgress(value.f1, progress));
	}
}
