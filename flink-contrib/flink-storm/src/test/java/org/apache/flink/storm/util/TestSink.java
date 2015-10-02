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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TestSink implements IRichBolt {
	private static final long serialVersionUID = 4314871456719370877L;

	public final static List<TopologyContext> result = new LinkedList<TopologyContext>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		result.add(context);
	}

	@Override
	public void execute(Tuple input) {
		if (input.size() == 1) {
			result.add((TopologyContext) input.getValue(0));
		} else {
			result.add((TopologyContext) input.getValue(1));
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
