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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * A test implementation of a {@link IRichBolt}.
 */
public class TestDummyBolt implements IRichBolt {
	private static final long serialVersionUID = 6893611247443121322L;

	public static final String SHUFFLE_STREAM_ID = "shuffleStream";
	public static final String GROUPING_STREAM_ID = "groupingStream";

	private boolean emit = true;
	@SuppressWarnings("rawtypes")
	public Map config;
	private TopologyContext context;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.config = stormConf;
		this.context = context;
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if (this.context.getThisTaskIndex() == 0) {
			this.collector.emit(SHUFFLE_STREAM_ID, input.getValues());
		}
		if (this.emit) {
			this.collector.emit(GROUPING_STREAM_ID, new Values("bolt", this.context));
			this.emit = false;
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(SHUFFLE_STREAM_ID, new Fields("data"));
		declarer.declareStream(GROUPING_STREAM_ID, new Fields("id", "data"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
