/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.util;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

/**
 * {@link NullTerminatingSpout} in a finite spout (ie, implements {@link FiniteSpout} interface) that wraps an
 * infinite spout, and returns {@code true} in {@link #reachedEnd()} when the wrapped spout does not emit a tuple
 * in {@code nextTuple()} for the first time.
 */
public class NullTerminatingSpout implements FiniteSpout {
	private static final long serialVersionUID = -6976210409932076066L;

	/** The original infinite Spout. */
	private final IRichSpout spout;
	/** The observer that checks if the given spouts emit a tuple or not on nextTuple(). */
	private SpoutOutputCollectorObserver observer;

	public NullTerminatingSpout(IRichSpout spout) {
		this.spout = spout;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.observer = new SpoutOutputCollectorObserver(collector);
		this.observer.emitted = true;
		this.spout.open(conf, context, this.observer);
	}

	@Override
	public void close() {
		this.spout.close();
	}

	@Override
	public void activate() {
		this.spout.activate();
	}

	@Override
	public void deactivate() {
		this.spout.deactivate();
	}

	@Override
	public void nextTuple() {
		this.observer.emitted = false;
		this.spout.nextTuple();
	}

	@Override
	public void ack(Object msgId) {
		this.spout.ack(msgId);
	}

	@Override
	public void fail(Object msgId) {
		this.spout.fail(msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.spout.declareOutputFields(declarer);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return this.spout.getComponentConfiguration();
	}

	@Override
	public boolean reachedEnd() {
		return !this.observer.emitted;
	}

}
