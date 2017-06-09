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

package org.apache.flink.storm.tests.operators;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * A Bolt implementation that verifies metadata emitted by a {@link MetaDataSpout}.
 */
public class VerifyMetaDataBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1353222852073800478L;

	public static final String STREAM_ID = "boltMeta";

	private OutputCollector collector;
	private TopologyContext context;

	public static boolean errorOccured = false;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.context = context;
	}

	@Override
	public void execute(Tuple input) {
		if (!input.getSourceComponent().equals(input.getString(0))
				|| !input.getSourceStreamId().equals(input.getString(1))
				|| !input.getSourceGlobalStreamid().get_componentId().equals(input.getString(0))
				|| !input.getSourceGlobalStreamid().get_streamId().equals(input.getString(1))
				|| input.getSourceTask() != input.getInteger(2).intValue()
				|| !input.getMessageId().equals(MessageId.makeUnanchored())
				|| input.getMessageId().getAnchors().size() != 0
				|| input.getMessageId().getAnchorsToIds().size() != 0) {
			errorOccured = true;
		}
		this.collector.emit(STREAM_ID, new Values(this.context.getThisComponentId(), STREAM_ID,
				this.context.getThisTaskId()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(STREAM_ID, new Fields("id", "sid", "tid"));
	}

}
