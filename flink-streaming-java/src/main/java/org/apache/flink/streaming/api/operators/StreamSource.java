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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link StreamOperator} for streaming sources.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function of this stream source operator
 */
@Internal
public class StreamSource<OUT, SRC extends SourceFunction<OUT>> 
		extends AbstractUdfStreamOperator<OUT, SRC> implements StreamOperator<OUT> {

	private static final long serialVersionUID = 1L;
	
	private transient SourceFunction.SourceContext<OUT> ctx;

	private transient volatile boolean canceledOrStopped = false;
	
	
	public StreamSource(SRC sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	public void run(final Object lockingObject) throws Exception {
		run(lockingObject, output);
	}

	
	public void run(final Object lockingObject, final Output<StreamRecord<OUT>> collector) throws Exception {
		final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();
		final long watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

		this.ctx = StreamSourceContexts.getSourceContext(
			timeCharacteristic, getTimerService(), lockingObject, collector, watermarkInterval);

		try {
			userFunction.run(ctx);

			// if we get here, then the user function either exited after being done (finite source)
			// or the function was canceled or stopped. For the finite source case, we should emit
			// a final watermark that indicates that we reached the end of event-time
			if (!isCanceledOrStopped()) {
				ctx.emitWatermark(Watermark.MAX_WATERMARK);
			}
		} finally {
			// make sure that the context is closed in any case
			ctx.close();
		}
	}

	public void cancel() {
		// important: marking the source as stopped has to happen before the function is stopped.
		// the flag that tracks this status is volatile, so the memory model also guarantees
		// the happens-before relationship
		markCanceledOrStopped();
		userFunction.cancel();
		
		// the context may not be initialized if the source was never running.
		if (ctx != null) {
			ctx.close();
		}
	}

	/**
	 * Marks this source as canceled or stopped.
	 * 
	 * <p>This indicates that any exit of the {@link #run(Object, Output)} method
	 * cannot be interpreted as the result of a finite source.  
	 */
	protected void markCanceledOrStopped() {
		this.canceledOrStopped = true;
	}
	
	/**
	 * Checks whether the source has been canceled or stopped. 
	 * @return True, if the source is canceled or stopped, false is not.
	 */
	protected boolean isCanceledOrStopped() {
		return canceledOrStopped;
	}
}
