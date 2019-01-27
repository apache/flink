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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.operators.StreamSourceV2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.InputSelector.InputSelection;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The type Source fetcher.
 */
class SourceFetcher implements InputFetcher {

	private final StreamSourceV2 operator;

	private final SourceInputProcessor processor;

	private final SourceFunction.SourceContext context;

	private final InputSelection inputSelection;

	private boolean isIdle = false;

	private volatile boolean finishedInput = false;

	private InputFetcherAvailableListener listener;

	/**
	 * Instantiates a new Source fetcher.
	 *
	 * @param operator  the operator
	 * @param context   the context
	 * @param processor the processor
	 */
	public SourceFetcher(
		InputSelection inputSelection,
		StreamSourceV2 operator,
		SourceFunction.SourceContext context,
		SourceInputProcessor processor) {

		this.inputSelection = checkNotNull(inputSelection);
		this.operator = checkNotNull(operator);
		this.processor = checkNotNull(processor);
		this.context = checkNotNull(context);
	}

	@Override
	public void setup() throws Exception {

	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean fetchAndProcess() throws Exception {
		if (isFinished()) {
			finishInput();
			return false;
		}
		final SourceRecord sourceRecord = operator.next();
		if (sourceRecord != null) {
			final Object out = sourceRecord.getRecord();
			if (sourceRecord.getWatermark() != null) {
				context.emitWatermark(sourceRecord.getWatermark());
			}
			if (out != null) {
				if (sourceRecord.getTimestamp() > 0) {
					context.collectWithTimestamp(out, sourceRecord.getTimestamp());
				} else {
					context.collect(out);
				}
			}
			isIdle = false;
		} else {
			context.markAsTemporarilyIdle();
			isIdle = true;
		}
		if (isFinished()) {
			finishInput();
			return false;
		}

		// TODO: return !idle status, register a timer
		return !isIdle;
	}

	private void finishInput() throws Exception {
		if (!finishedInput) {
			context.emitWatermark(Watermark.MAX_WATERMARK);
			synchronized (context.getCheckpointLock()) {
				processor.endInput();
				processor.release();
			}
			finishedInput = true;
		}
	}

	@Override
	public boolean isFinished() {
		return operator.isFinished();
	}

	@Override
	public boolean moreAvailable() {
		// TODO: register a timer to trigger available
		return !isFinished();
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void cancel() {
		operator.cancel();
	}

	@Override
	public InputSelection getInputSelection() {
		return inputSelection;
	}

	@Override
	public void registerAvailableListener(InputFetcherAvailableListener listener) {
		checkState(this.listener == null);
		this.listener = listener;
	}
}

