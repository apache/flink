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

package org.apache.flink.storm.split.operators;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * A Spout implementation that emits random numbers, optionally splitting them into odd/even streams.
 */
public class RandomSpout extends BaseRichSpout {
	private static final long serialVersionUID = -3978554318742509334L;

	public static final String EVEN_STREAM = "even";
	public static final String ODD_STREAM = "odd";

	private final boolean split;
	private Random r = new Random();
	private SpoutOutputCollector collector;

	public RandomSpout(boolean split, long seed) {
		this.split = split;
		this.r = new Random(seed);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		int i = r.nextInt();
		if (split) {
			if (i % 2 == 0) {
				this.collector.emit(EVEN_STREAM, new Values(i));
			} else {
				this.collector.emit(ODD_STREAM, new Values(i));
			}
		} else {
			this.collector.emit(new Values(i));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields schema = new Fields("number");
		if (split) {
			declarer.declareStream(EVEN_STREAM, schema);
			declarer.declareStream(ODD_STREAM, schema);
		} else {
			declarer.declare(schema);
		}
	}

}
