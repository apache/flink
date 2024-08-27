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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * Base interface for all stream data sources in Flink. The contract of a stream source is the
 * following: When the source should start emitting elements, the {@link #run} method is called with
 * a {@link SourceContext} that can be used for emitting elements. The run method can run for as
 * long as necessary. The source must, however, react to an invocation of {@link #cancel()} by
 * breaking out of its main loop.
 *
 * <h3>CheckpointedFunction Sources</h3>
 *
 * <p>Sources that also implement the {@link
 * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction} interface must ensure that state
 * checkpointing, updating of internal state and emission of elements are not done concurrently.
 * This is achieved by using the provided checkpointing lock object to protect update of state and
 * emission of elements in a synchronized block.
 *
 * <p>This is the basic pattern one should follow when implementing a checkpointed source:
 *
 * <pre>{@code
 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      private transient ListState<Long> checkpointedCount;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              // this synchronized block ensures that state checkpointing,
 *              // internal state updates and emission of elements are an atomic operation
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
 *      public void initializeState(FunctionInitializationContext context) {
 *          this.checkpointedCount = context
 *              .getOperatorStateStore()
 *              .getListState(new ListStateDescriptor<>("count", Long.class));
 *
 *          if (context.isRestored()) {
 *              for (Long count : this.checkpointedCount.get()) {
 *                  this.count += count;
 *              }
 *          }
 *      }
 *
 *      public void snapshotState(FunctionSnapshotContext context) {
 *          this.checkpointedCount.clear();
 *          this.checkpointedCount.add(count);
 *      }
 * }
 * }</pre>
 *
 * <h3>Timestamps and watermarks:</h3>
 *
 * <p>Sources may assign timestamps to elements and may manually emit watermarks via the methods
 * {@link SourceContext#collectWithTimestamp(Object, long)} and {@link
 * SourceContext#emitWatermark(Watermark)}.
 *
 * @param <T> The type of the elements produced by this source.
 * @deprecated This interface will be removed in future versions. Use the new {@link
 *     org.apache.flink.api.connector.source.Source} interface instead. NOTE: All sub-tasks from
 *     FLINK-28045 must be closed before this API can be completely removed.
 */
@Deprecated
@Public
public interface SourceFunction<T> extends Function, Serializable {

    /**
     * Starts the source. Implementations use the {@link SourceContext} to emit elements. Sources
     * that checkpoint their state for fault tolerance should use the {@link
     * SourceContext#getCheckpointLock() checkpoint lock} to ensure consistency between the
     * bookkeeping and emitting the elements.
     *
     * <p>Sources that implement {@link CheckpointedFunction} must lock on the {@link
     * SourceContext#getCheckpointLock() checkpoint lock} checkpoint lock (using a synchronized
     * block) before updating internal state and emitting elements, to make both an atomic
     * operation.
     *
     * <p>Refer to the {@link SourceFunction top-level class docs} for an example.
     *
     * @param ctx The context to emit elements to and for accessing locks.
     */
    void run(SourceContext<T> ctx) throws Exception;

    /**
     * Cancels the source. Most sources will have a while loop inside the {@link
     * #run(SourceContext)} method. The implementation needs to ensure that the source will break
     * out of that loop after this method is called.
     *
     * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
     * {@code false} in this method. That flag is checked in the loop condition.
     *
     * <p>In case of an ungraceful shutdown (cancellation of the source operator, possibly for
     * failover), the thread that calls {@link #run(SourceContext)} will also be {@link
     * Thread#interrupt() interrupted}) by the Flink runtime, in order to speed up the cancellation
     * (to ensure threads exit blocking methods fast, like I/O, blocking queues, etc.). The
     * interruption happens strictly after this method has been called, so any interruption handler
     * can rely on the fact that this method has completed (for example to ignore exceptions that
     * happen after cancellation).
     *
     * <p>During graceful shutdown (for example stopping a job with a savepoint), the program must
     * cleanly exit the {@link #run(SourceContext)} method soon after this method was called. The
     * Flink runtime will NOT interrupt the source thread during graceful shutdown. Source
     * implementors must ensure that no thread interruption happens on any thread that emits records
     * through the {@code SourceContext} from the {@link #run(SourceContext)} method; otherwise the
     * clean shutdown may fail when threads are interrupted while processing the final records.
     *
     * <p>Because the {@code SourceFunction} cannot easily differentiate whether the shutdown should
     * be graceful or ungraceful, we recommend that implementors refrain from interrupting any
     * threads that interact with the {@code SourceContext} at all. You can rely on the Flink
     * runtime to interrupt the source thread in case of ungraceful cancellation. Any additionally
     * spawned threads that directly emit records through the {@code SourceContext} should use a
     * shutdown method that does not rely on thread interruption.
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
         * Emits one element from the source, without attaching a timestamp. In most cases, this is
         * the default way of emitting elements.
         *
         * <p>The element will have no timestamp initially. If timestamps and watermarks are
         * required, for example for event-time windows, timers, or joins, then you need to assign a
         * timestamp via {@link DataStream#assignTimestampsAndWatermarks(WatermarkStrategy)} and set
         * a strategy that assigns timestamps (for example using {@link
         * WatermarkStrategy#withTimestampAssigner(TimestampAssignerSupplier)}).
         *
         * @param element The element to emit
         */
        void collect(T element);

        /**
         * Emits one element from the source, and attaches the given timestamp.
         *
         * @param element The element to emit
         * @param timestamp The timestamp in milliseconds since the Epoch
         */
        @PublicEvolving
        void collectWithTimestamp(T element, long timestamp);

        /**
         * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
         * elements with a timestamp {@code t' <= t} will occur any more. If further such elements
         * will be emitted, those elements are considered <i>late</i>.
         *
         * @param mark The Watermark to emit
         */
        @PublicEvolving
        void emitWatermark(Watermark mark);

        /**
         * Marks the source to be temporarily idle. This tells the system that this source will
         * temporarily stop emitting records and watermarks for an indefinite amount of time.
         *
         * <p>Source functions should make a best effort to call this method as soon as they
         * acknowledge themselves to be idle. The system will consider the source to resume activity
         * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T,
         * long)}, or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or
         * watermarks from the source.
         */
        @PublicEvolving
        void markAsTemporarilyIdle();

        /**
         * Returns the checkpoint lock. Please refer to the class-level comment in {@link
         * SourceFunction} for details about how to write a consistent checkpointed source.
         *
         * @return The object to use as the lock
         */
        Object getCheckpointLock();

        /** This method is called by the system to shut down the context. */
        void close();
    }
}
