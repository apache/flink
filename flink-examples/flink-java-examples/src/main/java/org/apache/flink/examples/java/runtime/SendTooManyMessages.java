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

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.messages.KeyValueMessage;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.types.LongValue;


/**
 * Implements how to communicate between parallel instances of a task at runtime.
 * This program must fail. We're sending messages more than the queue is configured to handle.
 */
@SuppressWarnings("serial")
public class SendTooManyMessages {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.generateSequence(1,100).filter(new RichFilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {
				getRuntimeContext().broadcast(new KeyValueMessage("", new LongValue(value)));
				return false;
			}
		}).output(new DiscardingOutputFormat<Long>());
		env.execute();
	}
}
