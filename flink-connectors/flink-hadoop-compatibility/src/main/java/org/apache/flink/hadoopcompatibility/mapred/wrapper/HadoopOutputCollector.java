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

package org.apache.flink.hadoopcompatibility.mapred.wrapper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 * A Hadoop OutputCollector that wraps a Flink OutputCollector.
 * On each call of collect() the data is forwarded to the wrapped Flink collector.
 */
public final class HadoopOutputCollector<KEY, VALUE> implements OutputCollector<KEY, VALUE> {

	private Collector<Tuple2<KEY, VALUE>> flinkCollector;

	private final Tuple2<KEY, VALUE> outTuple = new Tuple2<KEY, VALUE>();

	/**
	 * Set the wrapped Flink collector.
	 *
	 * @param flinkCollector The wrapped Flink OutputCollector.
	 */
	public void setFlinkCollector(Collector<Tuple2<KEY, VALUE>> flinkCollector) {
		this.flinkCollector = flinkCollector;
	}

	/**
	 * Use the wrapped Flink collector to collect a key-value pair for Flink.
	 *
	 * @param key the key to collect
	 * @param val the value to collect
	 * @throws IOException unexpected of key or value in key-value pair.
	 */
	@Override
	public void collect(final KEY key, final VALUE val) throws IOException {
		this.outTuple.f0 = key;
		this.outTuple.f1 = val;
		this.flinkCollector.collect(outTuple);
	}
}
