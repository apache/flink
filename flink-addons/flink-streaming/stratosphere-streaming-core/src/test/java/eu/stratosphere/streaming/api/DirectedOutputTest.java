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
package eu.stratosphere.streaming.api;

import static org.junit.Assert.*;

import org.junit.Test;

import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.configuration.Configuration;

public class DirectedOutputTest {

	private static class PlusOne extends MapFunction<Tuple1<Long>, Tuple1<Long>> {
		@Override
		public Tuple1<Long> map(Tuple1<Long> arg0) throws Exception {
			arg0.f0++;
			return arg0;
		}
	}

	@Test
	public void namingTest() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStream<Tuple1<Long>> s1 = env.generateSequence(1, 10);

		DataStream<Tuple1<Long>> ds1 = s1.map(new PlusOne()).name("ds1");
		DataStream<Tuple1<Long>> ds2 = s1.map(new PlusOne());
		DataStream<Tuple1<Long>> ds3 = s1.map(new PlusOne()).name("ds3");
		;

		System.out.println(env.jobGraphBuilder.edgeList);
		System.out.println(env.jobGraphBuilder.userDefinedNames);

		Configuration configS1 = env.jobGraphBuilder.components.get(s1.getId()).getConfiguration();

		assertEquals("ds1", configS1.getString("outputName_0", ""));
		assertEquals("", configS1.getString("outputName_1", ""));
		assertEquals("ds3", configS1.getString("outputName_2", ""));

		ds2.name("ds2");
		assertEquals("ds2", configS1.getString("outputName_1", ""));

		DataStream<Tuple1<Long>> s2 = env.generateSequence(11, 20);
		Configuration configS2 = env.jobGraphBuilder.components.get(s2.getId()).getConfiguration();

		DataStream<Tuple1<Long>> ds4 = s1.connectWith(s2).map(new PlusOne());
		DataStream<Tuple1<Long>> ds5 = s1.connectWith(s2).map(new PlusOne()).name("ds5");
		;

		assertEquals("", configS2.getString("outputName_0", ""));
		assertEquals("ds5", configS2.getString("outputName_1", ""));

		ds4.name("ds4");
		assertEquals("ds4", configS2.getString("outputName_0", ""));
	}
}
