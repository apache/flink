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
import org.apache.flink.api.common.messages.HeartBeatMessage;
import org.apache.flink.api.common.messages.KeyValueMessage;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.types.LongValue;

import java.util.List;

/**
 * Implements how to communicate between parallel instances of a task at runtime.
 *
 */
@SuppressWarnings("serial")
public class SendTwoMessages {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.generateSequence(1,20).filter(new RichFilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {
				getRuntimeContext().broadcast(new KeyValueMessage("", new LongValue(value)));
				return true;
			}
		}).filter(new RichFilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {
				getRuntimeContext().broadcast(new HeartBeatMessage());
				return false;
			}

			@Override
			public void close(){
				try {
					// wait for a while for others to finish to get all messages
					Thread.sleep(1000);
				} catch(InterruptedException e){

				}
				// let's fetch the messages now
				List messages = getRuntimeContext().receive();
				if(messages.size() != 40){
					throw new RuntimeException("Invalid count of messages received.");
				}
				int keyValue = 0;
				int heartBeat = 0;
				for(int i = 0; i < 40; i++){
					if(messages.get(i) instanceof KeyValueMessage){
						keyValue++;
					} else if(messages.get(i) instanceof HeartBeatMessage){
						heartBeat++;
					}
				}
				if(keyValue != 20 || heartBeat != 20){
					throw new RuntimeException("Invalid count of messages received.");
				}
			}
		}).output(new DiscardingOutputFormat<Long>());
		env.execute();
	}
}
