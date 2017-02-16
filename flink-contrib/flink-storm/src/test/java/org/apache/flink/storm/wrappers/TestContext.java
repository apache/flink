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

package org.apache.flink.storm.wrappers;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.LinkedList;

class TestContext implements SourceContext<Tuple1<Integer>> {
	public LinkedList<Tuple1<Integer>> result = new LinkedList<Tuple1<Integer>>();

	public TestContext() {
	}

	@Override
	public void collect(final Tuple1<Integer> record) {
		this.result.add(record.copy());
	}

	@Override
	public void collectWithTimestamp(Tuple1<Integer> element, long timestamp) {
		this.result.add(element.copy());
	}

	@Override
	public void emitWatermark(Watermark mark) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void markAsTemporarilyIdle() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getCheckpointLock() {
		return null;
	}

	@Override
	public void close() {

	}
}
