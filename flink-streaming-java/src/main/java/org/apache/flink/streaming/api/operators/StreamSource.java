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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link StreamOperator} for streaming sources.
 */
public class StreamSource<T> extends AbstractUdfStreamOperator<T, SourceFunction<T>> implements StreamOperator<T> {

	private static final long serialVersionUID = 1L;
	private transient SourceFunction.SourceContext<T> ctx;

	public StreamSource(SourceFunction<T> sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	public void run(final Object lockingObject, final Output<StreamRecord<T>> collector) throws Exception {
		final ExecutionConfig executionConfig = getExecutionConfig();
		
		if (userFunction instanceof EventTimeSourceFunction) {
			ctx = new ManualWatermarkContext<T>(lockingObject, collector, getRuntimeContext().getExecutionConfig().areTimestampsEnabled());
		} else if (executionConfig.getAutoWatermarkInterval() > 0) {
			ctx = new AutomaticWatermarkContext<T>(lockingObject, collector, executionConfig);
		} else if (executionConfig.areTimestampsEnabled()) {
			ctx = new NonWatermarkContext<T>(lockingObject, collector);
		} else {
			ctx = new NonTimestampContext<T>(lockingObject, collector);
		}

		userFunction.run(ctx);

		// This will mostly emit a final +Inf Watermark to make the Watermark logic work
		// when some sources finish before others do
		ctx.close();

		if (executionConfig.areTimestampsEnabled()) {
			synchronized (lockingObject) {
				output.emitWatermark(new Watermark(Long.MAX_VALUE));
			}
		}
	}

	public void cancel() {
		userFunction.cancel();
		// the context may not be initialized if the source was never running.
		if (ctx != null) {
			ctx.close();
		}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources that don't emit watermarks.
	 * In addition to {@link NonWatermarkContext} this will also not attach timestamps to sources.
	 * (Technically it will always set the timestamp to 0).
	 */
	public static class NonTimestampContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		public NonTimestampContext(Object lockingObjectParam, Output<StreamRecord<T>> outputParam) {
			this.lockingObject = lockingObjectParam;
			this.output = outputParam;
			this.reuse = new StreamRecord<T>(null);
		}

		@Override
		public void collect(T element) {
			output.collect(reuse.replace(element, 0));
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface EventTimeSourceFunction" +
					" if you want to manually assign timestamps to elements.");
		}

		@Override
		public void emitWatermark(Watermark mark) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface EventTimeSourceFunction" +
					" if you want to manually assign timestamps to elements.");
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}

		@Override
		public void close() {}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources that don't emit watermarks.
	 */
	public static class NonWatermarkContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		public NonWatermarkContext(Object lockingObjectParam, Output<StreamRecord<T>> outputParam) {
			this.lockingObject = lockingObjectParam;
			this.output = outputParam;
			this.reuse = new StreamRecord<T>(null);
		}

		@Override
		public void collect(T element) {
			long currentTime = System.currentTimeMillis();
			output.collect(reuse.replace(element, currentTime));
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface EventTimeSourceFunction" +
					" if you want to manually assign timestamps to elements.");
		}

		@Override
		public void emitWatermark(Watermark mark) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface EventTimeSourceFunction" +
					" if you want to manually assign timestamps to elements.");
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

		private final ScheduledExecutorService scheduleExecutor;
		private final ScheduledFuture<?> watermarkTimer;
		private final long watermarkInterval;

		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private volatile long lastWatermarkTime;

		public AutomaticWatermarkContext(Object lockingObjectParam,
				Output<StreamRecord<T>> outputParam,
				ExecutionConfig executionConfig) {
			this.lockingObject = lockingObjectParam;
			this.output = outputParam;
			this.reuse = new StreamRecord<T>(null);

			watermarkInterval = executionConfig.getAutoWatermarkInterval();

			scheduleExecutor = Executors.newScheduledThreadPool(1);

			watermarkTimer = scheduleExecutor.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					long currentTime = System.currentTimeMillis();
					// align the watermarks across all machines. this will ensure that we
					// don't have watermarks that creep along at different intervals because
					// the machine clocks are out of sync
					long watermarkTime = currentTime - (currentTime % watermarkInterval);
					if (currentTime > watermarkTime && watermarkTime - lastWatermarkTime >= watermarkInterval) {
						synchronized (lockingObject) {
							if (currentTime > watermarkTime && watermarkTime - lastWatermarkTime >= watermarkInterval) {
								output.emitWatermark(new Watermark(watermarkTime));
								lastWatermarkTime = watermarkTime;
							}
						}
					}
				}
			}, 0, watermarkInterval, TimeUnit.MILLISECONDS);

		}

		@Override
		public void collect(T element) {
			synchronized (lockingObject) {
				long currentTime = System.currentTimeMillis();
				output.collect(reuse.replace(element, currentTime));

				long watermarkTime = currentTime - (currentTime % watermarkInterval);
				if (currentTime > watermarkTime && watermarkTime - lastWatermarkTime >= watermarkInterval) {
					output.emitWatermark(new Watermark(watermarkTime));
					lastWatermarkTime = watermarkTime;
				}
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface EventTimeSourceFunction" +
					" if you want to manually assign timestamps to elements.");
		}

		@Override
		public void emitWatermark(Watermark mark) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface EventTimeSourceFunction" +
					" if you want to manually assign timestamps to elements.");
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}

		@Override
		public void close() {
			watermarkTimer.cancel(true);
			scheduleExecutor.shutdownNow();
			// emit one last +Inf watermark to make downstream watermark processing work
			// when some sources close early
			synchronized (lockingObject) {
				output.emitWatermark(new Watermark(Long.MAX_VALUE));
			}
		}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with manual timestamp
	 * assignment and manual watermark emission.
	 */
	public static class ManualWatermarkContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;
		private final boolean watermarkMultiplexingEnabled;

		public ManualWatermarkContext(Object lockingObject, Output<StreamRecord<T>> output, boolean watermarkMultiplexingEnabled) {
			this.lockingObject = lockingObject;
			this.output = output;
			this.reuse = new StreamRecord<T>(null);
			this.watermarkMultiplexingEnabled = watermarkMultiplexingEnabled;
		}

		@Override
		public void collect(T element) {
			throw new UnsupportedOperationException("Manual-Timestamp sources can only emit" +
					" elements with a timestamp. Please use collectWithTimestamp().");
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			synchronized (lockingObject) {
				output.collect(reuse.replace(element, timestamp));
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			if (watermarkMultiplexingEnabled) {
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
