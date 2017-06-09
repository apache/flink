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
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * A test implementation of a {@link IRichSpout}.
 */
public class TestDummySpout implements IRichSpout {
	private static final long serialVersionUID = -5190945609124603118L;

	public static final String SPOUT_STREAM_ID = "spout-stream";

	private boolean emit = true;
	@SuppressWarnings("rawtypes")
	public Map config;
	private TopologyContext context;
	private SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.config = conf;
		this.context = context;
		this.collector = collector;
	}

	@Override
	public void close() {}

	@Override
	public void activate() {}

	@Override
	public void deactivate() {}

	@Override
	public void nextTuple() {
		if (this.emit) {
			this.collector.emit(new Values(this.context));
			this.emit = false;
		}
	}

	@Override
	public void ack(Object msgId) {}

	@Override
	public void fail(Object msgId) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Utils.DEFAULT_STREAM_ID, new Fields("data"));
		declarer.declareStream(SPOUT_STREAM_ID, new Fields("id", "data"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
