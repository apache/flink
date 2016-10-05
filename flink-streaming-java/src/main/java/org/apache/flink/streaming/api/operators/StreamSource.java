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
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ScheduledFuture;

/**
 * {@link StreamOperator} for streaming sources.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function of this stream source operator
 */
@Internal
public class StreamSource<OUT, SRC extends SourceFunction<OUT>> 
		extends AbstractUdfStreamOperator<OUT, SRC> implements StreamOperator<OUT>, AsyncExceptionChecker {

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
		final SourceFunction.SourceContext<OUT> ctx;
		
		switch (timeCharacteristic) {
			case EventTime:
				ctx = new ManualWatermarkContext<>(this, lockingObject, collector);
				break;
			case IngestionTime:
				ctx = new AutomaticWatermarkContext<>(this, lockingObject, collector,
						getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval());
				break;
			case ProcessingTime:
				ctx = new NonTimestampContext<>(this, lockingObject, collector);
				break;
			default:
				throw new Exception("Invalid time characteristic: " + String.valueOf(timeCharacteristic));
		}

		// copy to a field to give the 'cancel()' method access
		this.ctx = ctx;
		
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

	/**
	 * Checks whether any asynchronous thread (checkpoint trigger, timer, watermark generator, ...)
	 * has caused an exception. If one of these threads caused an exception, this method will
	 * throw that exception.
	 */
	@Override
	public void checkAsyncException() {
		getContainingTask().checkTimerException();
	}

	// ------------------------------------------------------------------------
	//  Source contexts for various stream time characteristics
	// ------------------------------------------------------------------------
	
	/**
	 * A source context that attached {@code -1} as a timestamp to all records, and that
	 * does not forward watermarks.
	 */
	public static class NonTimestampContext<T> implements SourceFunction.SourceContext<T> {

		private final AsyncExceptionChecker owner;
		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		public NonTimestampContext(AsyncExceptionChecker owner, Object lockingObject, Output<StreamRecord<T>> output) {
			this.owner = owner;
			this.lockingObject = lockingObject;
			this.output = output;
			this.reuse = new StreamRecord<T>(null);
		}

		@Override
		public void collect(T element) {
			owner.checkAsyncException();
			synchronized (lockingObject) {
				output.collect(reuse.replace(element));
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			// ignore the timestamp
			collect(element);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			owner.checkAsyncException();
			// do nothing else
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}

		@Override
		public void close() {}
	}
	
	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with automatic timestamps
	 * and watermark emission.
	 */
	public static class AutomaticWatermarkContext<T> implements SourceFunction.SourceContext<T> {

		private final AbstractStreamOperator<T> owner;
		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;
		private final AsyncExceptionChecker source;

		private final ScheduledFuture<?> watermarkTimer;
		private final long watermarkInterval;

		private volatile long nextWatermarkTime;

		public AutomaticWatermarkContext(
				final AbstractStreamOperator<T> owner,
				final Object lockingObjectParam,
				final Output<StreamRecord<T>> outputParam,
				final long watermarkInterval) {
			
			if (watermarkInterval < 1L) {
				throw new IllegalArgumentException("The watermark interval cannot be smaller than one.");
			}

			this.owner = owner;
			this.lockingObject = lockingObjectParam;
			this.output = outputParam;
			this.watermarkInterval = watermarkInterval;
			this.reuse = new StreamRecord<T>(null);

			// if it is a source, then we cast and cache it
			// here so that we do not have to do it in every collect(),
			// collectWithTimestamp() and emitWatermark()

			if (!(owner instanceof AsyncExceptionChecker)) {
				throw new IllegalStateException("The AutomaticWatermarkContext can only be used " +
					"with sources that implement the AsyncExceptionChecker interface.");
			}
			this.source = (AsyncExceptionChecker) owner;

			long now = owner.getCurrentProcessingTime();
			this.watermarkTimer = owner.registerTimer(now + watermarkInterval,
				new WatermarkEmittingTask(owner, lockingObjectParam, outputParam));
		}

		@Override
		public void collect(T element) {
			source.checkAsyncException();
			
			synchronized (lockingObject) {
				final long currentTime = owner.getCurrentProcessingTime();
				output.collect(reuse.replace(element, currentTime));

				// this is to avoid lock contention in the lockingObject by
				// sending the watermark before the firing of the watermark
				// emission task.

				if (currentTime > nextWatermarkTime) {
					// in case we jumped some watermarks, recompute the next watermark time
					final long watermarkTime = currentTime - (currentTime % watermarkInterval);
					nextWatermarkTime = watermarkTime + watermarkInterval;
					output.emitWatermark(new Watermark(watermarkTime));

					// we do not need to register another timer here
					// because the emitting task will do so.
				}
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			collect(element);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			source.checkAsyncException();
			
			if (mark.getTimestamp() == Long.MAX_VALUE) {
				// allow it since this is the special end-watermark that for example the Kafka source emits
				synchronized (lockingObject) {
					nextWatermarkTime = Long.MAX_VALUE;
					output.emitWatermark(mark);
				}

				// we can shutdown the timer now, no watermarks will be needed any more
				if (watermarkTimer != null) {
					watermarkTimer.cancel(true);
				}
			}
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}

		@Override
		public void close() {
			if (watermarkTimer != null) {
				watermarkTimer.cancel(true);
			}
		}

		private class WatermarkEmittingTask implements Triggerable {

			private final AbstractStreamOperator<T> owner;
			private final Object lockingObject;
			private final Output<StreamRecord<T>> output;

			private WatermarkEmittingTask(AbstractStreamOperator<T> src, Object lock, Output<StreamRecord<T>> output) {
				this.owner = src;
				this.lockingObject = lock;
				this.output = output;
			}

			@Override
			public void trigger(long timestamp) {
				final long currentTime = owner.getCurrentProcessingTime();

				if (currentTime > nextWatermarkTime) {
					// align the watermarks across all machines. this will ensure that we
					// don't have watermarks that creep along at different intervals because
					// the machine clocks are out of sync
					final long watermarkTime = currentTime - (currentTime % watermarkInterval);

					synchronized (lockingObject) {
						if (currentTime > nextWatermarkTime) {
							output.emitWatermark(new Watermark(watermarkTime));
							nextWatermarkTime = watermarkTime + watermarkInterval;
						}
					}
				}

				owner.registerTimer(owner.getCurrentProcessingTime() + watermarkInterval,
					new WatermarkEmittingTask(owner, lockingObject, output));
			}
		}
	}

	/**
	 * A SourceContext for event time. Sources may directly attach timestamps and generate
	 * watermarks, but if records are emitted without timestamps, no timestamps are automatically
	 * generated and attached. The records will simply have no timestamp in that case.
	 * 
	 * Streaming topologies can use timestamp assigner functions to override the timestamps
	 * assigned here.
	 */
	public static class ManualWatermarkContext<T> implements SourceFunction.SourceContext<T> {

		private final AsyncExceptionChecker owner;
		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		public ManualWatermarkContext(AsyncExceptionChecker owner, Object lockingObject, Output<StreamRecord<T>> output) {
			this.owner = owner;
			this.lockingObject = lockingObject;
			this.output = output;
			this.reuse = new StreamRecord<T>(null);
		}

		@Override
		public void collect(T element) {
			owner.checkAsyncException();
			synchronized (lockingObject) {
				output.collect(reuse.replace(element));
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			owner.checkAsyncException();
			synchronized (lockingObject) {
				output.collect(reuse.replace(element, timestamp));
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			owner.checkAsyncException();
			synchronized (lockingObject) {
				output.emitWatermark(mark);
			}
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}

		@Override
		public void close() {}
	}
}
