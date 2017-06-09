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

package org.apache.flink.storm.tests.operators;

import org.apache.flink.storm.util.FiniteSpout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * A Spout implementation that broadcasts random numbers across a specified number of output streams, until a specified
 * count is reached.
 */
public class FiniteRandomSpout extends BaseRichSpout implements FiniteSpout {
	private static final long serialVersionUID = 6592885571932363239L;

	public static final String STREAM_PREFIX = "stream_";

	private final Random r;
	private SpoutOutputCollector collector;
	private int counter;
	private final String[] outputStreams;

	public FiniteRandomSpout(long seed, int counter, int numberOfOutputStreams) {
		this.r = new Random(seed);
		this.counter = counter;
		if (numberOfOutputStreams < 1) {
			this.outputStreams = new String[] { Utils.DEFAULT_STREAM_ID };
		} else {
			this.outputStreams = new String[numberOfOutputStreams];
			for (int i = 0; i < this.outputStreams.length; ++i) {
				this.outputStreams[i] = STREAM_PREFIX + i;
			}
		}
	}

	public FiniteRandomSpout(long seed, int counter) {
		this(seed, counter, 1);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		for (String s : this.outputStreams) {
			this.collector.emit(s, new Values(this.r.nextInt()));
		}
		--this.counter;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for (String s : this.outputStreams) {
			declarer.declareStream(s, new Fields("number"));
		}
	}

	@Override
	public boolean reachedEnd() {
		return this.counter <= 0;
	}

}
