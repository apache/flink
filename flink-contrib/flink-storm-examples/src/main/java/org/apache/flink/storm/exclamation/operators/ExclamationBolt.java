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

package org.apache.flink.storm.exclamation.operators;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * A Bolt implementation that appends exclamation marks to incoming tuples. The number of added exclamation marks can
 * be controlled by setting <code>exclamation.count</code>.
 */
public class ExclamationBolt implements IRichBolt {
	private static final long serialVersionUID = -6364882114201311380L;

	public static final String EXCLAMATION_COUNT = "exclamation.count";

	private OutputCollector collector;
	private String exclamation;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		Object count = conf.get(EXCLAMATION_COUNT);
		if (count != null) {
			int exclamationNum = (Integer) count;
			StringBuilder builder = new StringBuilder();
			for (int index = 0; index < exclamationNum; ++index) {
				builder.append('!');
			}
			this.exclamation = builder.toString();
		} else {
			this.exclamation = "!";
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple tuple) {
		collector.emit(tuple, new Values(tuple.getString(0) + this.exclamation));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
