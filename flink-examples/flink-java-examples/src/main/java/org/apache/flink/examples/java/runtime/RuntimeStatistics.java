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

package org.apache.flink.examples.java.runtime;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.messages.KeyValueMessage;
import org.apache.flink.api.common.messages.TaskMessage;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Implements how to communicate between parallel instances of a task at runtime.
 *
 */
@SuppressWarnings("serial")
public class RuntimeStatistics {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.generateSequence(1,20).setParallelism(2).mapPartition(new RichMapPartitionFunction<Long, Long>() {
			@Override
			public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
				ArrayList<Long> list = new ArrayList<Long>();
				for(Long j: values){
					list.add(j);
				}
				for(int i = 0; i < 5; i++){
					for(Long elem: list){
						getRuntimeContext().broadcast(new KeyValueMessage("", new LongValue(elem)));
					}
					Thread.sleep(1000);
					long count = 0;
					for(TaskMessage elem: getRuntimeContext().receive()){
						count += ((LongValue) ((KeyValueMessage) elem).getValue()).getValue();
					}
					if(count != 210){
						throw new RuntimeException("Invalid sum.");
					}
				}
			}
		}).output(new DiscardingOutputFormat<Long>());
		env.execute();
	}
}
