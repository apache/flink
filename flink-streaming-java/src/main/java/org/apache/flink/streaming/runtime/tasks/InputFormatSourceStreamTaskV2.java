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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamTask} for executing a {@link StreamSourceV2}.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 */
@Internal
public class InputFormatSourceStreamTaskV2<OUT, SRC extends SourceFunctionV2<OUT>, OP extends StreamSourceV2<OUT, SRC>>
		extends StreamTask<OUT, OP> {

	private static final Logger LOG = LoggerFactory.getLogger(InputFormatSourceStreamTaskV2.class);

	private boolean running = true;

	public InputFormatSourceStreamTaskV2(Environment env) {
		super(env);
	}

	@Override
	protected void init() {
	}

	@Override
	protected void cleanup() {
		// does not hold any resources, so no cleanup needed
	}

	@Override
	protected void run() throws Exception {

		final OP headOperator = getHeadOperator();

		final TimeCharacteristic timeCharacteristic = headOperator.getOperatorConfig().getTimeCharacteristic();

		final Output<StreamRecord<OUT>> collector = headOperator.getOutput();

		final long watermarkInterval = headOperator.getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

		final SourceFunction.SourceContext<OUT> ctx = StreamSourceContexts.getSourceContext(
				timeCharacteristic,
				getProcessingTimeService(),
				getCheckpointLock(),
				getStreamStatusMaintainer(),
				collector,
				watermarkInterval,
				-1);

		try {
			while (running) {
				final SourceRecord<OUT> sourceRecord = headOperator.next();
				if (sourceRecord != null) {
					final OUT out = sourceRecord.getRecord();
					if (out != null) {
						ctx.collect(out);
					}
				} else {
					break;
				}
			}

			// if we get here, then the user function either exited after being done (finite source)
			// or the function was canceled or stopped. For the finite source case, we should emit
			// a final watermark that indicates that we reached the end of event-time
			if (running) {

				synchronized (getCheckpointLock()) {
					for (StreamOperator operator : operatorChain.getAllOperatorsTopologySorted()) {
						if (operator instanceof OneInputStreamOperator) {
							((OneInputStreamOperator) operator).endInput();
						}
					}
				}
			} else {
				headOperator.cancel();
				// the context may not be initialized if the source was never running.
			}
		} finally {
			// make sure that the context is closed in any case
			if (ctx != null) {
				ctx.close();
			}
		}
	}

	@Override
	protected void cancelTask() throws Exception {
		running = false;
	}

	@SuppressWarnings("unchecked")
	protected OP getHeadOperator() {
		Preconditions.checkState(operatorChain.getHeadOperators().length == 1,
				"There should only one head operator, not " + operatorChain.getHeadOperators().length);
		return (OP) operatorChain.getHeadOperators()[0];
	}
}
