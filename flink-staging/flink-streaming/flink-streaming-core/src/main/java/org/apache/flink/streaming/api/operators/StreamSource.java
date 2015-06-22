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

import org.apache.flink.streaming.api.functions.source.ManualTimestampSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.joda.time.Instant;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link StreamOperator} for streaming sources.
 */
public class StreamSource<T> extends AbstractUdfStreamOperator<T, SourceFunction<T>> implements StreamOperator<T> {

	private static final long serialVersionUID = 1L;

	public StreamSource(SourceFunction<T> sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	public void run(final Object lockingObject, final Output<StreamRecord<T>> collector) throws Exception {

		if (executionConfig.getAutoWatermarkInterval() != null && !(userFunction instanceof ManualTimestampSourceFunction)) {
			ScheduledExecutorService service = Executors.newScheduledThreadPool(2);

			service.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					synchronized (lockingObject) {
						output.emitWatermark(new Watermark(Instant.now()));
					}
				}
			}, 0, executionConfig.getAutoWatermarkInterval().getMillis(), TimeUnit.MILLISECONDS);

		}

		SourceFunction.SourceContext<T> ctx = null;
		if (userFunction instanceof ManualTimestampSourceFunction) {
			ctx = new ManualTimestampContext<T>(lockingObject, collector);
		} else {
			ctx = new AutomaticTimestampContext<T>(lockingObject, collector);
		}

		userFunction.run(ctx);
	}

	public void cancel() {
		userFunction.cancel();
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with automatic timestamps
	 * and watermark emission.
	 */
	public static class AutomaticTimestampContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		StreamRecord<T> reuse;

		public AutomaticTimestampContext(Object lockingObject, Output<StreamRecord<T>> output) {
			this.lockingObject = lockingObject;
			this.output = output;
			reuse = new StreamRecord<T>(null);
		}

		@Override
		public void collect(T element) {
			synchronized (lockingObject) {
				output.collect(reuse.replace(element, Instant.now()));
			}
		}

		@Override
		public void collectWithTimestamp(T element, Instant timestamp) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface ManualTimestampSourceFunction" +
					" if you want to manually assign timestamps to elements.");
		}

		@Override
		public void emitWatermark(Watermark mark) {
			throw new UnsupportedOperationException("Automatic-Timestamp sources cannot emit" +
					" elements with a timestamp. See interface ManualTimestampSourceFunction" +
					" if you want to manually assign timestamps to elements.");
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with manual timestamp
	 * assignment and manual watermark emission.
	 */
	public static class ManualTimestampContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lockingObject;
		private final Output<StreamRecord<T>> output;
		StreamRecord<T> reuse;

		public ManualTimestampContext(Object lockingObject, Output<StreamRecord<T>> output) {
			this.lockingObject = lockingObject;
			this.output = output;
			reuse = new StreamRecord<T>(null);
		}

		@Override
		public void collect(T element) {
			throw new UnsupportedOperationException("Manual-Timestamp sources can only emit" +
					" elements with a timestamp. Please use collectWithTimestamp().");
		}

		@Override
		public void collectWithTimestamp(T element, Instant timestamp) {
			synchronized (lockingObject) {
				output.collect(reuse.replace(element, timestamp));
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}

		@Override
		public Object getCheckpointLock() {
			return lockingObject;
		}
	}
}
