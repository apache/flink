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
package org.apache.flink.stormcompatibility.util;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class TestDummySpout implements IRichSpout {
	private static final long serialVersionUID = -5190945609124603118L;

	public final static String spoutStreamId = "spout-stream";

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {}

	@Override
	public void close() {}

	@Override
	public void activate() {}

	@Override
	public void deactivate() {}

	@Override
	public void nextTuple() {}

	@Override
	public void ack(Object msgId) {}

	@Override
	public void fail(Object msgId) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Utils.DEFAULT_STREAM_ID, new Fields("data"));
		declarer.declareStream(spoutStreamId, new Fields("id", "data"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
