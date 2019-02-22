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

package org.apache.flink.streaming.examples.broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Example illustrating broadcast.
 */
public class BroadcastExample {
	
	public static void main(String[] args) throws Exception {
		
		final List<Tuple2<String, String>> rule = new ArrayList<>();
		rule.add(new Tuple2<>("10", "20"));
		rule.add(new Tuple2<>("15", "50"));
		rule.add(new Tuple2<>("20", "100"));
		
		final List<Tuple2<String, String>> keyedInput = new ArrayList<>();
		keyedInput.add(new Tuple2<>("20190220", "5"));
		keyedInput.add(new Tuple2<>("20190221", "10"));
		keyedInput.add(new Tuple2<>("20190222", "5"));
		keyedInput.add(new Tuple2<>("20190223", "20"));
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<Tuple2<String, String>> elementStream = env.fromCollection(keyedInput).rebalance();
		
		MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>(
			"rulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
		);
		BroadcastStream<Tuple2<String, String>> broadcastStream = env.fromCollection(rule).broadcast(mapStateDescriptor);
		
		DataStream<Tuple2<String, String>> output = elementStream
			.connect(broadcastStream)
			.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
				
				private final MapStateDescriptor<String, String> broadcastStateDescriptor =
					new MapStateDescriptor<>("rulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
				
				private Tuple2<String, String> tuple2 = new Tuple2<>();
				
				@Override
				public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
					ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);
					if (broadcastState.get(value.f1) != null) {
						tuple2.f0 = value.f0;
						tuple2.f1 = value.f1 + "-match-" + broadcastState.get(value.f1);
					} else {
						tuple2.f0 = value.f0;
						tuple2.f1 = value.f1 + "-nomatch-" + broadcastState.get(value.f1);
					}
					out.collect(tuple2);
				}
				
				@Override
				public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
					System.out.println(value.f0 + "=========>" + value.f1);
					ctx.getBroadcastState(broadcastStateDescriptor).put(value.f0, value.f1);
				}
			});
		output.print("==========");
		
		env.execute();
	}
}
