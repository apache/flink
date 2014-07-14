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
import eu.stratosphere.streaming.api.SourceFunction;
import eu.stratosphere.util.Collector;

public class WindowJoinSourceOne extends SourceFunction<Tuple4<String, String, Integer, Long>> {

	private static final long serialVersionUID = 6670933703432267728L;

	private String[] names = { "tom", "jerry", "alice", "bob", "john", "grace", "sasa", "lawrance",
			"andrew", "jean", "richard", "smith", "gorge", "black", "peter" };
	private Random rand = new Random();
	private Tuple4<String, String, Integer, Long> outRecord = new Tuple4<String, String, Integer, Long>();
	private Long progress = 0L;

	@Override
	public void invoke(Collector<Tuple4<String, String, Integer, Long>> collector) throws Exception {
		while (true) {
			outRecord.f0 = "salary";
			outRecord.f1 = names[rand.nextInt(names.length)];
			outRecord.f2 = rand.nextInt(10000);
			outRecord.f3 = progress;
			collector.collect(outRecord);
			progress += 1;
		}
	}
}
