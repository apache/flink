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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.SimpleHistogram;
import org.apache.flink.runtime.metrics.SumAndCount;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.ScheduledFuture;

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

	private transient boolean enableTracingMetrics = false;

	private transient int tracingMetricsInterval;

	private transient SumAndCount taskLatency;

	private transient Histogram sourceLatency;

	public StreamSource(SRC sourceFunction) {
		super(sourceFunction);

		this.chainingStrategy = ChainingStrategy.HEAD;
	}

	public void run(final Object lockingObject, final StreamStatusMaintainer streamStatusMaintainer) throws Exception {
		run(lockingObject, streamStatusMaintainer, output);
	}

	public void run(final Object lockingObject,
			final StreamStatusMaintainer streamStatusMaintainer,
			final Output<StreamRecord<OUT>> collector) throws Exception {

		final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();
		enableTracingMetrics = getRuntimeContext().getExecutionConfig().isTracingMetricsEnabled();
		if (enableTracingMetrics) {
			if (taskLatency == null) {
				taskLatency = new SumAndCount(MetricNames.TASK_LATENCY, getRuntimeContext().getMetricGroup());
			}
			if (sourceLatency == null) {
				sourceLatency = getRuntimeContext().getMetricGroup().histogram(MetricNames.SOURCE_LATENCY, new SimpleHistogram());
			}
			tracingMetricsInterval = getRuntimeContext().getExecutionConfig().getTracingMetricsInterval();
		}

		LatencyMarksEmitter latencyEmitter = null;
		if (getExecutionConfig().isLatencyTrackingEnabled()) {
			latencyEmitter = new LatencyMarksEmitter<>(
				getProcessingTimeService(),
				collector,
				getExecutionConfig().getLatencyTrackingInterval(),
				this.getOperatorID(),
				getRuntimeContext().getIndexOfThisSubtask());
		}

		final long watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

		this.ctx = getSourceContext(
			timeCharacteristic,
			getProcessingTimeService(),
			lockingObject,
			streamStatusMaintainer,
			collector,
			watermarkInterval);

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
			if (latencyEmitter != null) {
				latencyEmitter.close();
			}
		}
	}

	@VisibleForTesting
	protected SourceFunction.SourceContext<OUT> getSourceContext(
		TimeCharacteristic timeCharacteristic,
		ProcessingTimeService processingTimeService,
		Object lockingObject,
		StreamStatusMaintainer streamStatusMaintainer,
		Output<StreamRecord<OUT>> collector,
		boolean enableTracingMetrics,
		int tracingMetricsInterval,
		SumAndCount taskLatency,
		Histogram sourceLatency,
		long watermarkInterval) {

		return getSourceContext(
			timeCharacteristic,
			processingTimeService,
			lockingObject,
			streamStatusMaintainer,
			getOutputWithTaskLatency(collector, enableTracingMetrics, tracingMetricsInterval, taskLatency, sourceLatency),
			watermarkInterval);
	}

	private SourceFunction.SourceContext<OUT> getSourceContext(
			TimeCharacteristic timeCharacteristic,
			ProcessingTimeService processingTimeService,
			Object lockingObject,
			StreamStatusMaintainer streamStatusMaintainer,
			Output<StreamRecord<OUT>> collector,
			long watermarkInterval) {

		return StreamSourceContexts.getSourceContext(
			timeCharacteristic,
			processingTimeService,
			lockingObject,
			streamStatusMaintainer,
			getOutputWithTaskLatency(collector, enableTracingMetrics, tracingMetricsInterval, taskLatency, sourceLatency),
			watermarkInterval,
			-1);
	}

	private Output<StreamRecord<OUT>> getOutputWithTaskLatency(
			Output<StreamRecord<OUT>> collector,
			boolean enableTracingMetrics,
			int tracingMetricsInterval,
			SumAndCount taskLatency,
			Histogram sourceLatency) {
		return new Output<StreamRecord<OUT>>() {
			private long lastEmitTime = 0;
			private long emitCounter = 0;

			@Override
			public void emitWatermark(Watermark mark) {
				collector.emitWatermark(mark);
			}

			@Override
			public void emitLatencyMarker(LatencyMarker latencyMarker) {
				collector.emitLatencyMarker(latencyMarker);
			}

			@Override
			public void collect(StreamRecord<OUT> record) {
				if (enableTracingMetrics && (emitCounter++ % tracingMetricsInterval == 0)) {
					collectWithMetrics(record);
				} else {
					collector.collect(record);
				}
			}

			public void collectWithMetrics(StreamRecord<OUT> record) {
				long start = System.nanoTime();

				if (lastEmitTime > 0) {
					sourceLatency.update(start - lastEmitTime);
				}

				collector.collect(record);

				lastEmitTime = System.nanoTime();
				taskLatency.update(lastEmitTime - start);
			}

			@Override
			public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
				if (enableTracingMetrics && (emitCounter++ % tracingMetricsInterval == 0)) {
					collectWithMetrics(outputTag, record);
				} else {
					collector.collect(outputTag, record);
				}
			}

			public <X> void collectWithMetrics(OutputTag<X> outputTag, StreamRecord<X> record) {
				long start = System.nanoTime();

				if (lastEmitTime > 0) {
					sourceLatency.update(start - lastEmitTime);
				}

				collector.collect(outputTag, record);

				lastEmitTime = System.nanoTime();
				taskLatency.update(lastEmitTime - start);
			}

			@Override
			public void close() {
				collector.close();
			}
		};
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
	 * <p>This indicates that any exit of the {@link #run(Object, StreamStatusMaintainer, Output)} method
	 * cannot be interpreted as the result of a finite source.
	 */
	protected void markCanceledOrStopped() {
		this.canceledOrStopped = true;
	}

	/**
	 * Checks whether the source has been canceled or stopped.
	 * @return True, if the source is canceled or stopped, false is not.
	 */
	public boolean isCanceledOrStopped() {
		return canceledOrStopped;
	}

	private static class LatencyMarksEmitter<OUT> {
		private final ScheduledFuture<?> latencyMarkTimer;

		public LatencyMarksEmitter(
				final ProcessingTimeService processingTimeService,
				final Output<StreamRecord<OUT>> output,
				long latencyTrackingInterval,
				final OperatorID operatorId,
				final int subtaskIndex) {

			latencyMarkTimer = processingTimeService.scheduleAtFixedRate(
				new ProcessingTimeCallback() {
					@Override
					public void onProcessingTime(long timestamp) throws Exception {
						try {
							// ProcessingTimeService callbacks are executed under the checkpointing lock
							output.emitLatencyMarker(new LatencyMarker(timestamp, operatorId, subtaskIndex));
						} catch (Throwable t) {
							// we catch the Throwables here so that we don't trigger the processing
							// timer services async exception handler
							LOG.warn("Error while emitting latency marker.", t);
						}
					}
				},
				0L,
				latencyTrackingInterval);
		}

		public void close() {
			latencyMarkTimer.cancel(true);
		}
	}
}
