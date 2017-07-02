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

import org.apache.flink.storm.util.FiniteInMemorySpout;
import org.apache.flink.storm.wordcount.util.WordCountData;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

/**
 * Implements a Spout that reads data from {@link WordCountData#WORDS}.
 */
public final class WordCountInMemorySpout extends FiniteInMemorySpout {
	private static final long serialVersionUID = 8832143302409465843L;

	public WordCountInMemorySpout() {
		super(WordCountData.WORDS);
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
}
