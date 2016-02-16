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
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * Base interface for all stream data sources in Flink. The contract of a stream source
 * is the following: When the source should start emitting elements the {@link #run} method
 * is called with a {@link org.apache.flink.util.Collector} that can be used for emitting elements.
 * The run method can run for as long as necessary. The source must, however, react to an
 * invocation of {@link #cancel} by breaking out of its main loop.
 *
 * <p>
 * <b>Note about checkpointed sources</b> <br>
 *
 * Sources that also implement the {@link org.apache.flink.streaming.api.checkpoint.Checkpointed}
 * interface must ensure that state checkpointing, updating of internal state and emission of
 * elements are not done concurrently. This is achieved by using the provided checkpointing lock
 * object to protect update of state and emission of elements in a synchronized block.
 * </p>
 *
 * <p>
 * This is the basic pattern one should follow when implementing a (checkpointed) source:
 * </p>
 *
 * <pre>{@code
 *  public class ExampleSource<T> implements SourceFunction<T>, Checkpointed<Long> {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      {@literal @}Override
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      {@literal @}Override
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      {@literal @}Override
 *      public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }
 *
 *      {@literal @}Override
 *      public void restoreState(Long state) { this.count = state; }
 * }
 * }</pre>
 *
 *
 * <p>
 * <b>Note about element timestamps and watermarks:</b> <br>
 * Sources must only manually emit watermarks when they implement
 * {@link EventTimeSourceFunction }.
 * Otherwise, elements automatically get the current timestamp assigned at ingress
 * and the system automatically emits watermarks.
 *
 * @param <T> The type of the elements produced by this source.
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

	/**
	 * Starts the source. You can use the {@link org.apache.flink.util.Collector} parameter to emit
	 * elements. Sources that implement
	 * {@link org.apache.flink.streaming.api.checkpoint.Checkpointed} must lock on the
	 * checkpoint lock (using a synchronized block) before updating internal state and/or emitting
	 * elements. Also, the update of state and emission of elements must happen in the same
	 * synchronized block.
	 *
	 * @param ctx The context for interaction with the outside world.
	 */
	void run(SourceContext<T> ctx) throws Exception;

	/**
	 * Cancels the source. Most sources will have a while loop inside the
	 * {@link #run} method. You need to ensure that the source will break out of this loop. This
	 * can be achieved by having a volatile field "isRunning" that is checked in the loop and that
	 * is set to false in this method.
	 */
	void cancel();

	/**
	 * Interface that source functions use to communicate with the outside world. Normally
	 * sources would just emit elements in a loop using {@link #collect}. If the source is a
	 * {@link org.apache.flink.streaming.api.checkpoint.Checkpointed} source it must retrieve
	 * the checkpoint lock object and use it to protect state updates and element emission as
	 * described in {@link org.apache.flink.streaming.api.functions.source.SourceFunction}.
	 *
	 * @param <T> The type of the elements produced by the source.
	 */
	@Public // Interface might be extended in the future with additional methods.
	public static interface SourceContext<T> {

		/**
		 * Emits one element from the source. The result of {@link System#currentTimeMillis()} is set as
		 * the timestamp of the emitted element.
		 *
		 * @param element The element to emit
		 */
		void collect(T element);

		/**
		 * Emits one element from the source with the given timestamp.
		 *
		 * @param element The element to emit
		 * @param timestamp The timestamp in milliseconds
		 */
		@PublicEvolving
		public void collectWithTimestamp(T element, long timestamp);

		/**
		 * Emits the given {@link org.apache.flink.streaming.api.watermark.Watermark}.
		 *
		 * <p>
		 * <b>Important:</b>
		 * Sources must only manually emit watermarks when they implement
		 * {@link EventTimeSourceFunction}.
		 * Otherwise, elements automatically get the current timestamp assigned at ingress
		 * and the system automatically emits watermarks.
		 *
		 * @param mark The {@link Watermark} to emit
		 */
		@PublicEvolving
		void emitWatermark(Watermark mark);


		/**
		 * Returns the checkpoint lock. Please refer to the explanation about checkpointed sources
		 * in {@link org.apache.flink.streaming.api.functions.source.SourceFunction}.
		 * 
		 * @return The object to use as the lock. 
		 */
		Object getCheckpointLock();

		/**
		 * This must be called when closing the source operator to allow the {@link SourceContext}
		 * to clean up internal state.
		 */
		void close();
	}
}
