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
package org.apache.flink.stormcompatibility.wordcount.stormoperators;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;





/**
 * Base class for Storm Spout that reads data line by line from an arbitrary source. The declared output schema has a
 * single attribute calle {@code line} and should be of type {@link String}.
 */
public abstract class AbstractStormSpout implements IRichSpout {
	private static final long serialVersionUID = 8876828403487806771L;
	
	public final static String ATTRIBUTE_LINE = "line";
	public final static int ATTRIBUTE_LINE_INDEX = 0;
	
	protected SpoutOutputCollector collector;
	
	
	
	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context, @SuppressWarnings("hiding") final SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void close() {/* noting to do */}
	
	@Override
	public void activate() {/* noting to do */}
	
	@Override
	public void deactivate() {/* noting to do */}
	
	@Override
	public void ack(final Object msgId) {/* noting to do */}
	
	@Override
	public void fail(final Object msgId) {/* noting to do */}
	
	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ATTRIBUTE_LINE));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}
