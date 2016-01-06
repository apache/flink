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

package org.apache.flink.tez.runtime.output;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;
import java.util.List;

public class TezOutputCollector<T> implements Collector<T> {

	private List<KeyValueWriter> writers;

	private List<TezChannelSelector<T>> outputEmitters;

	private List<Integer> numberOfStreamsInOutputs;

	private int numOutputs;

	private TypeSerializer<T> serializer;

	public TezOutputCollector(List<KeyValueWriter> writers, List<TezChannelSelector<T>> outputEmitters, TypeSerializer<T> serializer, List<Integer> numberOfStreamsInOutputs) {
		this.writers = writers;
		this.outputEmitters = outputEmitters;
		this.numberOfStreamsInOutputs = numberOfStreamsInOutputs;
		this.serializer = serializer;
		this.numOutputs = writers.size();
	}

	@Override
	public void collect(T record) {
		for (int i = 0; i < numOutputs; i++) {
			KeyValueWriter writer = writers.get(i);
			TezChannelSelector<T> outputEmitter = outputEmitters.get(i);
			int numberOfStreamsInOutput = numberOfStreamsInOutputs.get(i);
			try {
				for (int channel : outputEmitter.selectChannels(record, numberOfStreamsInOutput)) {
					IntWritable key = new IntWritable(channel);
					writer.write(key, record);
				}
			}
			catch (IOException e) {
				throw new RuntimeException("Emitting the record caused an I/O exception: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public void close() {

	}
}
