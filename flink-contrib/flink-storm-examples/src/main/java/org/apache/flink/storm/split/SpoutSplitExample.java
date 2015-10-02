/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.storm.split;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.storm.split.operators.RandomSpout;
import org.apache.flink.storm.split.operators.VerifyAndEnrichBolt;
import org.apache.flink.storm.util.SplitStreamMapper;
import org.apache.flink.storm.util.SplitStreamType;
import org.apache.flink.storm.util.StormStreamSelector;
import org.apache.flink.storm.wrappers.BoltWrapper;
import org.apache.flink.storm.wrappers.SpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Implements a simple example with two declared output streams for the embedded spout.
 * <p/>
 * This example shows how to:
 * <ul>
 * <li>handle multiple output stream of a spout</li>
 * <li>accessing each stream by .split(...) and .select(...)</li>
 * <li>strip wrapper data type SplitStreamType for further processing in Flink</li>
 * </ul>
 * <p/>
 * This example would work the same way for multiple bolt output streams.
 */
public class SpoutSplitExample {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String[] rawOutputs = new String[] { RandomSpout.EVEN_STREAM, RandomSpout.ODD_STREAM };

		final DataStream<SplitStreamType<Integer>> numbers = env.addSource(
				new SpoutWrapper<SplitStreamType<Integer>>(new RandomSpout(true, 0),
						rawOutputs), TypeExtractor.getForObject(new SplitStreamType<Integer>()));

		SplitStream<SplitStreamType<Integer>> splitStream = numbers
				.split(new StormStreamSelector<Integer>());

		DataStream<SplitStreamType<Integer>> evenStream = splitStream.select(RandomSpout.EVEN_STREAM);
		DataStream<SplitStreamType<Integer>> oddStream = splitStream.select(RandomSpout.ODD_STREAM);

		evenStream.map(new SplitStreamMapper<Integer>()).returns(Integer.class).map(new Enrich("even")).print();
		oddStream.transform("oddBolt",
				TypeExtractor.getForObject(new Tuple2<String, Integer>("", 0)),
				new BoltWrapper<SplitStreamType<Integer>, Tuple2<String, Integer>>(
						new VerifyAndEnrichBolt(false)))
						.print();

		// execute program
		env.execute("Spout split stream example");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Same as {@link VerifyAndEnrichBolt}.
	 */
	private final static class Enrich implements MapFunction<Integer, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 5213888269197438892L;
		private final Tuple2<String, Integer> out;

		public Enrich(String token) {
			this.out = new Tuple2<String, Integer>(token, 0);
		}

		@Override
		public Tuple2<String, Integer> map(Integer value) throws Exception {
			this.out.setField(value, 1);
			return this.out;
		}
	}

}
