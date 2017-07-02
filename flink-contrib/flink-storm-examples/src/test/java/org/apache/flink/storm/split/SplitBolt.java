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

package org.apache.flink.storm.split;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * A bolt for splitting an input stream containing numbers based on whether they are even or odd.
 */
public class SplitBolt extends BaseRichBolt {
	private static final long serialVersionUID = -6627606934204267173L;

	public static final String EVEN_STREAM = "even";
	public static final String ODD_STREAM = "odd";

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		if (input.getInteger(0) % 2 == 0) {
			this.collector.emit(EVEN_STREAM, new Values(input.getInteger(0)));
		} else {
			this.collector.emit(ODD_STREAM, new Values(input.getInteger(0)));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields schema = new Fields("number");
		declarer.declareStream(EVEN_STREAM, schema);
		declarer.declareStream(ODD_STREAM, schema);
	}

}
