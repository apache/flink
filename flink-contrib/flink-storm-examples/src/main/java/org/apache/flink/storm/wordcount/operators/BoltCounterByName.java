/*
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
 */

package org.apache.flink.storm.wordcount.operators;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Implements the word counter that counts the occurrence of each unique word. The bolt takes a pair (input tuple
 * schema: {@code <String,Integer>}) and sums the given word count for each unique word (output tuple schema:
 * {@code <String,Integer>} ).
 *
 * <p>Same as {@link BoltCounter}, but accesses input attribute by name (instead of index).
 */
public class BoltCounterByName implements IRichBolt {
	private static final long serialVersionUID = 399619605462625934L;

	public static final String ATTRIBUTE_WORD = "word";
	public static final String ATTRIBUTE_COUNT = "count";

	private final HashMap<String, Count> counts = new HashMap<String, Count>();
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(final Tuple input) {
		final String word = input.getStringByField(BoltTokenizer.ATTRIBUTE_WORD);

		Count currentCount = this.counts.get(word);
		if (currentCount == null) {
			currentCount = new Count();
			this.counts.put(word, currentCount);
		}
		currentCount.count += input.getIntegerByField(BoltTokenizer.ATTRIBUTE_COUNT);

		this.collector.emit(new Values(word, currentCount.count));
	}

	@Override
	public void cleanup() {/* nothing to do */}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ATTRIBUTE_WORD, ATTRIBUTE_COUNT));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**
	 * A counter helper to emit immutable tuples to the given stormCollector and avoid unnecessary object
	 * creating/deletion.
	 */
	private static final class Count {
		public int count;

		public Count() {/* nothing to do */}
	}

}
