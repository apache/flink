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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * Base interface for all stream data sources in Flink. The contract of a stream source
 * is the following: When the source should start emitting elements, the {@link #run} method
 * is called with a {@link SourceContext} that can be used for emitting elements.
 * The run method can run for as long as necessary. The source must, however, react to an
 * invocation of {@link #cancel()} by breaking out of its main loop.
 *
 * <h3>Checkpointed Sources</h3>
 *
 * <p>Sources that also implement the {@link org.apache.flink.streaming.api.checkpoint.Checkpointed}
 * interface must ensure that state checkpointing, updating of internal state and emission of
 * elements are not done concurrently. This is achieved by using the provided checkpointing lock
 * object to protect update of state and emission of elements in a synchronized block.
 *
 * <p>This is the basic pattern one should follow when implementing a (checkpointed) source:
 *
 * <pre>{@code
 *  public class ExampleSource<T> implements SourceFunction<T>, Checkpointed<Long> {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }
 *
 *      public void restoreState(Long state) { this.count = state; }
 * }
 * }</pre>
 *
 *
 * <h3>Timestamps and watermarks:</h3>
 * 
 * Sources may assign timestamps to elements and may manually emit watermarks.
 * However, these are only interpreted if the streaming program runs on 
 * {@link TimeCharacteristic#EventTime}. On other time characteristics 
 * ({@link TimeCharacteristic#IngestionTime} and {@link TimeCharacteristic#ProcessingTime}),
 * the watermarks from the source function are ignored.
 *
 * <h3>Gracefully Stopping Functions</h3>
 * 
 * Functions may additionally implement the {@link org.apache.flink.api.common.functions.StoppableFunction}
 * interface. "Stopping" a function, in contrast to "canceling" means a graceful exit that leaves the
 * state and the emitted elements in a consistent state.
 * 
 * <p>When a source is stopped, the executing thread is not interrupted, but expected to leave the
 * {@link #run(SourceContext)} method in reasonable time on its own, preserving the atomicity
 * of state updates and element emission. 
 * 
 * @param <T> The type of the elements produced by this source.
 * 
 * @see org.apache.flink.api.common.functions.StoppableFunction
 * @see org.apache.flink.streaming.api.TimeCharacteristic
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

	/**
	 * Starts the source. Implementations can use the {@link SourceContext} emit
	 * elements.
	 * 
	 * <p>Sources that implement {@link org.apache.flink.streaming.api.checkpoint.Checkpointed}
	 * must lock on the checkpoint lock (using a synchronized block) before updating internal
	 * state and emitting elements, to make both an atomic operation:
	 *
	 * <pre>{@code
	 *  public class ExampleSource<T> implements SourceFunction<T>, Checkpointed<Long> {
	 *      private long count = 0L;
	 *      private volatile boolean isRunning = true;
	 *
	 *      public void run(SourceContext<T> ctx) {
	 *          while (isRunning && count < 1000) {
	 *              synchronized (ctx.getCheckpointLock()) {
	 *                  ctx.collect(count);
	 *                  count++;
	 *              }
	 *          }
	 *      }
	 *
	 *      public void cancel() {
	 *          isRunning = false;
	 *      }
	 *
	 *      public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }
	 *
	 *      public void restoreState(Long state) { this.count = state; }
	 * }
	 * }</pre>
	 *
	 * @param ctx The context to emit elements to and for accessing locks.
	 */
	void run(SourceContext<T> ctx) throws Exception;

	/**
	 * Cancels the source. Most sources will have a while loop inside the
	 * {@link #run(SourceContext)} method. The implementation needs to ensure that the
	 * source will break out of that loop after this method is called.
	 * 
	 * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
	 * {@code false} in this method. That flag is checked in the loop condition.
	 * 
	 * <p>When a source is canceled, the executing thread will also be interrupted
	 * (via {@link Thread#interrupt()}). The interruption happens strictly after this
	 * method has been called, so any interruption handler can rely on the fact that
	 * this method has completed. It is good practice to make any flags altered by
	 * this method "volatile", in order to guarantee the visibility of the effects of
	 * this method to any interruption handler.
	 */
	void cancel();

	// ------------------------------------------------------------------------
	//  source context
	// ------------------------------------------------------------------------
	
	/**
	 * Interface that source functions use to emit elements, and possibly watermarks.
	 *
	 * @param <T> The type of the elements produced by the source.
	 */
	@Public // Interface might be extended in the future with additional methods.
	interface SourceContext<T> {

		/**
		 * Emits one element from the source, without attaching a timestamp. In most cases,
		 * this is the default way of emitting elements.
		 * 
		 * <p>The timestamp that the element will get assigned depends on the time characteristic of
		 * the streaming program:
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the element has no timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the element gets the system's
		 *         current time as the timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the element will have no timestamp initially.
		 *         It needs to get a timestamp (via a {@link TimestampAssigner}) before any time-dependent
		 *         operation (like time windows).</li>
		 * </ul>
		 *
		 * @param element The element to emit
		 */
		void collect(T element);

		/**
		 * Emits one element from the source, and attaches the given timestamp. This method
		 * is relevant for programs using {@link TimeCharacteristic#EventTime}, where the
		 * sources assign timestamps themselves, rather than relying on a {@link TimestampAssigner}
		 * on the stream.
		 *
		 * <p>On certain time characteristics, this timestamp may be ignored or overwritten.
		 * This allows programs to switch between the different time characteristics and behaviors
		 * without changing the code of the source functions.
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the timestamp will be ignored,
		 *         because processing time never works with element timestamps.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the timestamp is overwritten with the
		 *         system's current time, to realize proper ingestion time semantics.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the timestamp will be used.</li>
		 * </ul>
		 *
		 * @param element The element to emit
		 * @param timestamp The timestamp in milliseconds since the Epoch
		 */
		@PublicEvolving
		void collectWithTimestamp(T element, long timestamp);

		/**
		 * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
		 * elements with a timestamp {@code t' <= t} will occur any more. If further such
		 * elements will be emitted, those elements are considered <i>late</i>.
		 * 
		 * <p>This method is only relevant when running on {@link TimeCharacteristic#EventTime}.
		 * On {@link TimeCharacteristic#ProcessingTime},Watermarks will be ignored. On
		 * {@link TimeCharacteristic#IngestionTime}, the Watermarks will be replaced by the
		 * automatic ingestion time watermarks.
		 *
		 * @param mark The Watermark to emit
		 */
		@PublicEvolving
		void emitWatermark(Watermark mark);


		/**
		 * Returns the checkpoint lock. Please refer to the class-level comment in
		 * {@link SourceFunction} for details about how to write a consistent checkpointed
		 * source.
		 * 
		 * @return The object to use as the lock 
		 */
		Object getCheckpointLock();

		/**
		 * This method is called by the system to shut down the context.
		 */
		void close();
	}
}
