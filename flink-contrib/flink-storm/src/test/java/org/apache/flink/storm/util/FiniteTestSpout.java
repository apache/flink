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

package org.apache.flink.storm.util;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Tests for the Finite.
 */
public class FiniteTestSpout implements IRichSpout {
	private static final long serialVersionUID = 7992419478267824279L;

	private int numberOfOutputTuples;
	private SpoutOutputCollector collector;

	public FiniteTestSpout(final int numberOfOutputTuples) {
		this.numberOfOutputTuples = numberOfOutputTuples;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {/* nothing to do */}

	@Override
	public void activate() {/* nothing to do */}

	@Override
	public void deactivate() {/* nothing to do */}

	@Override
	public void nextTuple() {
		if (--this.numberOfOutputTuples >= 0) {
			this.collector.emit(new Values(new Integer(this.numberOfOutputTuples)));
		}
	}

	@Override
	public void ack(final Object msgId) {/* nothing to do */}

	@Override
	public void fail(final Object msgId) {/* nothing to do */}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("dummy"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
