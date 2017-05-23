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
import org.apache.storm.tuple.Tuple;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A test implementation of a {@link IRichBolt} that stores incoming records in {@link #RESULT}.
 */
public class TestSink implements IRichBolt {
	private static final long serialVersionUID = 4314871456719370877L;

	public static final List<TopologyContext> RESULT = new LinkedList<TopologyContext>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		RESULT.add(context);
	}

	@Override
	public void execute(Tuple input) {
		if (input.size() == 1) {
			RESULT.add((TopologyContext) input.getValue(0));
		} else {
			RESULT.add((TopologyContext) input.getValue(1));
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
