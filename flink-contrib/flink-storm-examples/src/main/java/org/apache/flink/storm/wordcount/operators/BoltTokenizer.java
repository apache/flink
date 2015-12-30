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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Implements the string tokenizer that splits sentences into words as a bolt. The bolt takes a line (input tuple
 * schema: {@code <String>}) and splits it into multiple pairs in the form of "(word,1)" (output tuple schema:
 * {@code <String,Integer>}).
 * <p>
 * Same as {@link BoltTokenizerByName}, but accesses input attribute by index (instead of name).
 */
public final class BoltTokenizer implements IRichBolt {
	private static final long serialVersionUID = -8589620297208175149L;

	public static final String ATTRIBUTE_WORD = "word";
	public static final String ATTRIBUTE_COUNT = "count";

	public static final int ATTRIBUTE_WORD_INDEX = 0;
	public static final int ATTRIBUTE_COUNT_INDEX = 1;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(final Tuple input) {
		final String[] tokens = input.getString(0).toLowerCase().split("\\W+");

		for (final String token : tokens) {
			if (token.length() > 0) {
				this.collector.emit(new Values(token, 1));
			}
		}
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

}
