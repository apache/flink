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

import java.util.Random;

import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class WindowJoinSourceTwo implements SourceFunction<Tuple4<String, String, Integer, Long>> {

	private static final long serialVersionUID = -5897483980082089771L;

	private String[] names = { "tom", "jerry", "alice", "bob", "john", "grace", "sasa", "lawrance",
			"andrew", "jean", "richard", "smith", "gorge", "black", "peter" };
	private Random rand = new Random();
	private Tuple4<String, String, Integer, Long> outTuple = new Tuple4<String, String, Integer, Long>();
	private Long progress = 0L;

	@Override
	public void invoke(Collector<Tuple4<String, String, Integer, Long>> out) throws Exception {
		// Continuously emit tuples with random names and integers (grades).
		while (true) {
			outTuple.f0 = "grade";
			outTuple.f1 = names[rand.nextInt(names.length)];
			outTuple.f2 = rand.nextInt(5) + 1;
			outTuple.f3 = progress;
			out.collect(outTuple);
			progress += 1;
		}
	}
}
