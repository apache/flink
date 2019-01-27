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

package org.apache.flink.table.runtime;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.GeneratedProcessFunction;
import org.apache.flink.table.runtime.functions.ExecutionContextImpl;
import org.apache.flink.table.runtime.functions.ProcessFunction;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} for executing keyed
 * {@link ProcessFunction ProcessFunctions}.
 */
public class KeyedProcessOperator<K, IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT>, Triggerable<Object, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private GeneratedProcessFunction funcCode;

	private ProcessFunction<IN, OUT> function;

	private transient StreamRecordCollector<OUT> collector;

	private transient ContextImpl context;

	private transient OnTimerContextImpl onTimerContext;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean functionsClosed = false;

	public KeyedProcessOperator(GeneratedProcessFunction funcCode) {
		this.funcCode = funcCode;

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	public KeyedProcessOperator(ProcessFunction<IN, OUT> function) {
		this.function = function;

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		if (funcCode != null) {
			Class<ProcessFunction<IN, OUT>> functionClass =
				CodeGenUtils.compile(
					getContainingTask().getUserCodeClassLoader(),
					funcCode.name(),
					funcCode.code());
			function = functionClass.newInstance();
		}
		function.open(new ExecutionContextImpl(this, getRuntimeContext()));

		collector = new StreamRecordCollector<>(output);

		InternalTimerService<VoidNamespace> internalTimerService =
			getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

		TimerService timerService = new SimpleTimerService(internalTimerService);

		context = new ContextImpl(timerService);
		onTimerContext = new OnTimerContextImpl(timerService);
	}


	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);

		if (function instanceof CheckpointListener) {
			((CheckpointListener) function).notifyCheckpointComplete(checkpointId);
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), function);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		StreamingFunctionUtils.restoreFunctionState(context, function);
	}

	@Override
	public void close() throws Exception {
		super.close();
		functionsClosed = true;
		function.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (!functionsClosed) {
			functionsClosed = true;
			function.close();
		}
	}

	@Override
	public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		setCurrentKey(timer.getKey());

		onTimerContext.timeDomain = TimeDomain.EVENT_TIME;
		function.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
	}

	@Override
	public void onProcessingTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		setCurrentKey(timer.getKey());

		onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
		function.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		function.processElement(element.getValue(), context, collector);
	}

	@Override
	public void endInput() throws Exception {
		function.endInput(collector);
	}

	private class ContextImpl extends ProcessFunction.Context {

		private final TimerService timerService;

		ContextImpl(TimerService timerService) {
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}
	}

	private class OnTimerContextImpl extends ProcessFunction.OnTimerContext{

		private final TimerService timerService;

		private TimeDomain timeDomain;

		OnTimerContextImpl(TimerService timerService) {
			this.timerService = checkNotNull(timerService);
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public TimeDomain timeDomain() {
			checkState(timeDomain != null);
			return timeDomain;
		}
	}
}
