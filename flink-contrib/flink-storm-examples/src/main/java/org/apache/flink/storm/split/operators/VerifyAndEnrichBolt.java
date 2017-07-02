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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Verifies that incoming numbers are either even or odd, controlled by the constructor argument. Emitted tuples are
 * enriched with a new string field containing either "even" or "odd", based on the number's parity.
 */
public class VerifyAndEnrichBolt extends BaseRichBolt {
	private static final long serialVersionUID = -7277395570966328721L;

	private final boolean evenOrOdd; // true: even -- false: odd
	private final String token;
	private OutputCollector collector;

	public static boolean errorOccured = false;

	public VerifyAndEnrichBolt(boolean evenOrOdd) {
		this.evenOrOdd = evenOrOdd;
		this.token = evenOrOdd ? "even" : "odd";
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		if ((input.getInteger(0) % 2 == 0) != this.evenOrOdd) {
			errorOccured = true;
		}
		this.collector.emit(new Values(this.token, input.getInteger(0)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("evenOrOdd", "number"));
	}

}
