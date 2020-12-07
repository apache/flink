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

package org.apache.flink.state.api.output.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.output.SnapshotUtils;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.runtime.VoidTriggerable;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.function.Supplier;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing {@link
 * KeyedStateBootstrapFunction}'s.
 */
@Internal
public class KeyedStateBootstrapOperator<K, IN>
	extends AbstractUdfStreamOperator<TaggedOperatorSubtaskState, KeyedStateBootstrapFunction<K, IN>>
	implements OneInputStreamOperator<IN, TaggedOperatorSubtaskState>,
	BoundedOneInput {

	private static final long serialVersionUID = 1L;

	private final long timestamp;

	private final Path savepointPath;

	private transient KeyedStateBootstrapOperator<K, IN>.ContextImpl context;

	public KeyedStateBootstrapOperator(long timestamp, Path savepointPath, KeyedStateBootstrapFunction<K, IN> function) {
		super(function);

		this.timestamp = timestamp;
		this.savepointPath = savepointPath;
	}

	@Override
	public void open() throws Exception {
		super.open();

		Supplier<InternalTimerService<VoidNamespace>> internalTimerService = () -> getInternalTimerService(
			"user-timers",
			VoidNamespaceSerializer.INSTANCE,
			VoidTriggerable.instance());

		TimerService timerService = new LazyTimerService(internalTimerService, getProcessingTimeService());

		context = new KeyedStateBootstrapOperator<K, IN>.ContextImpl(userFunction, timerService);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		userFunction.processElement(element.getValue(), context);
	}

	@Override
	public void endInput() throws Exception {
		TaggedOperatorSubtaskState state = SnapshotUtils.snapshot(
			this,
			getRuntimeContext().getIndexOfThisSubtask(),
			timestamp,
			getContainingTask().getConfiguration().isExactlyOnceCheckpointMode(),
			getContainingTask().getConfiguration().isUnalignedCheckpointsEnabled(),
			getContainingTask().getCheckpointStorage(),
			savepointPath);

		output.collect(new StreamRecord<>(state));
	}

	private class ContextImpl extends KeyedStateBootstrapFunction<K, IN>.Context {

		private final TimerService timerService;

		ContextImpl(KeyedStateBootstrapFunction<K, IN> function, TimerService timerService) {
			function.super();
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		@SuppressWarnings("unchecked")
		public K getCurrentKey() {
			return (K) KeyedStateBootstrapOperator.this.getCurrentKey();
		}
	}
}
