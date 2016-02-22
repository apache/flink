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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

	public StreamSource(SRC sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	public void run(final Object lockingObject, final Output<StreamRecord<OUT>> collector) throws Exception {
		final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();
		
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
				throw new Exception(String.valueOf(timeCharacteristic));
		}

		userFunction.run(ctx);

		ctx.close();
	}

	public void cancel() {
		userFunction.cancel();
		// the context may not be initialized if the source was never running.
		if (ctx != null) {
			ctx.close();
		}
	}
	
	void checkAsyncException() {
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

		private final StreamSource<?, ?> owner;
		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		public NonTimestampContext(StreamSource<?, ?> owner, Object lockingObject, Output<StreamRecord<T>> output) {
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

		private final StreamSource<?, ?> owner;
		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;
		
		private final ScheduledExecutorService scheduleExecutor;
		private final ScheduledFuture<?> watermarkTimer;
		private final long watermarkInterval;

		private volatile long nextWatermarkTime;

		public AutomaticWatermarkContext(
				final StreamSource<?, ?> owner,
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
			
			this.scheduleExecutor = Executors.newScheduledThreadPool(1);

			this.watermarkTimer = scheduleExecutor.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					final long currentTime = System.currentTimeMillis();
					
					if (currentTime > nextWatermarkTime) {
						// align the watermarks across all machines. this will ensure that we
						// don't have watermarks that creep along at different intervals because
						// the machine clocks are out of sync
						final long watermarkTime = currentTime - (currentTime % watermarkInterval);
						
						synchronized (lockingObjectParam) {
							if (currentTime > nextWatermarkTime) {
								outputParam.emitWatermark(new Watermark(watermarkTime));
								nextWatermarkTime += watermarkInterval;
							}
						}
					}
				}
			}, 0, watermarkInterval, TimeUnit.MILLISECONDS);
		}

		@Override
		public void collect(T element) {
			owner.checkAsyncException();
			
			synchronized (lockingObject) {
				final long currentTime = System.currentTimeMillis();
				output.collect(reuse.replace(element, currentTime));
				
				if (currentTime > nextWatermarkTime) {
					// in case we jumped some watermarks, recompute the next watermark time
					final long watermarkTime = currentTime - (currentTime % watermarkInterval);
					nextWatermarkTime = watermarkTime + watermarkInterval;
					output.emitWatermark(new Watermark(watermarkTime));
				}
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			collect(element);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			owner.checkAsyncException();
			
			if (mark.getTimestamp() == Long.MAX_VALUE) {
				// allow it since this is the special end-watermark that for example the Kafka source emits
				synchronized (lockingObject) {
					nextWatermarkTime = Long.MAX_VALUE;
					output.emitWatermark(mark);
				}

				// we can shutdown the timer now, no watermarks will be needed any more
				watermarkTimer.cancel(true);
				scheduleExecutor.shutdownNow();
			}
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}

		@Override
		public void close() {
			watermarkTimer.cancel(true);
			scheduleExecutor.shutdownNow();
		}
	}

	/**
	 * A SourceContext for event time. Sources may directly attach timestamps and generate
	 * watermarks, but if records are emitted without timestamps, no timetamps are automatically
	 * generated and attached. The records will simply have no timestamp in that case.
	 * 
	 * Streaming topologies can use timestamp assigner functions to override the timestamps
	 * assigned here.
	 */
	public static class ManualWatermarkContext<T> implements SourceFunction.SourceContext<T> {

		private final StreamSource<?, ?> owner;
		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		public ManualWatermarkContext(StreamSource<?, ?> owner, Object lockingObject, Output<StreamRecord<T>> output) {
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
