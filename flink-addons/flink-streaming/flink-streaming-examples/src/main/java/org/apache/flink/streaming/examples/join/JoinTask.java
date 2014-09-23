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

package org.apache.flink.streaming.examples.join;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.function.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

//Joins the input value with the already known values. If it is a grade
// then with the salaries, if it is a salary then with the grades. Also
// stores the new element.
public class JoinTask extends
		RichCoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>> {
	private static final long serialVersionUID = 1L;

	private HashMap<String, ArrayList<Integer>> gradeHashmap;
	private HashMap<String, ArrayList<Integer>> salaryHashmap;
	private String name;

	public JoinTask() {
		gradeHashmap = new HashMap<String, ArrayList<Integer>>();
		salaryHashmap = new HashMap<String, ArrayList<Integer>>();
		name = new String();
	}

	Tuple3<String, Integer, Integer> outputTuple = new Tuple3<String, Integer, Integer>();

	// GRADES
	@Override
	public void flatMap1(Tuple2<String, Integer> value,
			Collector<Tuple3<String, Integer, Integer>> out) {
		name = value.f0;
		outputTuple.f0 = name;
		outputTuple.f1 = value.f1;
		if (salaryHashmap.containsKey(name)) {
			for (Integer salary : salaryHashmap.get(name)) {
				outputTuple.f2 = salary;
				out.collect(outputTuple);
			}
		}
		if (!gradeHashmap.containsKey(name)) {
			gradeHashmap.put(name, new ArrayList<Integer>());
		}
		gradeHashmap.get(name).add(value.f1);
	}

	// SALARIES
	@Override
	public void flatMap2(Tuple2<String, Integer> value,
			Collector<Tuple3<String, Integer, Integer>> out) {
		name = value.f0;
		outputTuple.f0 = name;
		outputTuple.f2 = value.f1;
		if (gradeHashmap.containsKey(name)) {
			for (Integer grade : gradeHashmap.get(name)) {
				outputTuple.f1 = grade;
				out.collect(outputTuple);
			}
		}
		if (!salaryHashmap.containsKey(name)) {
			salaryHashmap.put(name, new ArrayList<Integer>());
		}
		salaryHashmap.get(name).add(value.f1);
	}
}
