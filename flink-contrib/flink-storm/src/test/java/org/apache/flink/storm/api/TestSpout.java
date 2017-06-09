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

package org.apache.flink.storm.api;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

/**
 * A no-op test implementation of a {@link IRichSpout}.
 */
public class TestSpout implements IRichSpout {
	private static final long serialVersionUID = -4884029383198924007L;

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
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
