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

import java.util.Map;

/**
 * Implements a sink that write the received data so some external output. The result is formatted like
 * {@code (a1, a2, ..., an)} with {@code Object.toString()} for each attribute).
 */
public abstract class AbstractBoltSink implements IRichBolt {
	private static final long serialVersionUID = -1626323806848080430L;

	private StringBuilder lineBuilder;
	private String prefix = "";
	private final OutputFormatter formatter;

	public AbstractBoltSink(final OutputFormatter formatter) {
		this.formatter = formatter;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public final void prepare(final Map stormConf, final TopologyContext context,
			final OutputCollector collector) {
		this.prepareSimple(stormConf, context);
		if (context.getComponentCommon(context.getThisComponentId()).get_parallelism_hint() > 1) {
			this.prefix = context.getThisTaskId() + "> ";
		}
	}

	protected abstract void prepareSimple(Map<?, ?> stormConf, TopologyContext context);

	@Override
	public final void execute(final Tuple input) {
		this.lineBuilder = new StringBuilder();
		this.lineBuilder.append(this.prefix);
		this.lineBuilder.append(this.formatter.format(input));
		this.writeExternal(this.lineBuilder.toString());
	}

	protected abstract void writeExternal(String line);

	@Override
	public void cleanup() {/* nothing to do */}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer declarer) {/* nothing to do */}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
