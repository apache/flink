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
import org.apache.storm.utils.Utils;

import java.util.List;

/**
 * Observes if a call to any {@code emit(...)} or {@code emitDirect(...)} method is made.
 * The internal flag {@link #emitted} must be reset by the user manually.
 */
class SpoutOutputCollectorObserver extends SpoutOutputCollector {

	/** The collector to be observed. */
	private final SpoutOutputCollector delegate;
	/** The internal flag that it set to {@code true} if a tuple gets emitted. */
	boolean emitted;

	public SpoutOutputCollectorObserver(SpoutOutputCollector delegate) {
		super(null);
		this.delegate = delegate;
	}

	@Override
	public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
		emitted = true;
		return this.delegate.emit(streamId, tuple, messageId);
	}

	@Override
	public List<Integer> emit(List<Object> tuple, Object messageId) {
		return emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}

	@Override
	public List<Integer> emit(List<Object> tuple) {
		return emit(tuple, null);
	}

	@Override
	public List<Integer> emit(String streamId, List<Object> tuple) {
		return emit(streamId, tuple, null);
	}

	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
		emitted = true;
		delegate.emitDirect(taskId, streamId, tuple, messageId);
	}

	@Override
	public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
		emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
	}

	@Override
	public void emitDirect(int taskId, String streamId, List<Object> tuple) {
		emitDirect(taskId, streamId, tuple, null);
	}

	@Override
	public void emitDirect(int taskId, List<Object> tuple) {
		emitDirect(taskId, tuple, null);
	}

	@Override
	public void reportError(Throwable error) {
		delegate.reportError(error);
	}

}
