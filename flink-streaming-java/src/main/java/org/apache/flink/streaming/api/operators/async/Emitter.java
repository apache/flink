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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.queue.AsyncCollectionResult;
import org.apache.flink.streaming.api.operators.async.queue.AsyncResult;
import org.apache.flink.streaming.api.operators.async.queue.AsyncWatermarkResult;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.execution.MailboxExecutor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * Runnable responsible for consuming elements from the given queue and outputting them to the
 * given output/timestampedCollector.
 *
 * @param <OUT> Type of the output elements
 */
@Internal
public class Emitter<OUT> implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(Emitter.class);

	/** Lock to hold before outputting. */
	private final Object checkpointLock;

	/** Output for the watermark elements. */
	private final Output<StreamRecord<OUT>> output;

	/** Executor for mailbox. */
	private final MailboxExecutor executor;

	/** Queue to consume the async results from. */
	private final StreamElementQueue streamElementQueue;

	private final OperatorActions operatorActions;

	/** Output for stream records. */
	private final TimestampedCollector<OUT> timestampedCollector;

	private volatile boolean running;

	public Emitter(
			final Object checkpointLock,
			final @Nonnull MailboxExecutor executor,
			final @Nonnull Output<StreamRecord<OUT>> output,
			final @Nonnull StreamElementQueue streamElementQueue,
			final @Nonnull OperatorActions operatorActions) {

		this.checkpointLock = Preconditions.checkNotNull(checkpointLock, "checkpointLock");
		this.executor = Preconditions.checkNotNull(executor, "executor");
		this.output = Preconditions.checkNotNull(output, "output");
		this.streamElementQueue = Preconditions.checkNotNull(streamElementQueue, "streamElementQueue");
		this.operatorActions = Preconditions.checkNotNull(operatorActions, "operatorActions");

		this.timestampedCollector = new TimestampedCollector<>(this.output);
		this.running = true;
	}

	@Override
	public void run() {
		try {
			while (running) {
				LOG.debug("Wait for next completed async stream element result.");
				AsyncResult asyncResult = streamElementQueue.peekBlockingly();
				executor.submit(() -> {
					try {
						output(asyncResult);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}).get();
			}
		} catch (InterruptedException e) {
			if (running) {
				operatorActions.failOperator(e);
			} else {
				// Thread got interrupted which means that it should shut down
				LOG.debug("Emitter thread got interrupted, shutting down.");
			}
		} catch (Throwable t) {
			operatorActions.failOperator(new Exception("AsyncWaitOperator's emitter caught an " +
				"unexpected throwable.", t));
		}
	}

	/**
	 * Executed as a mail in the mailbox thread. Output needs to be guarded with checkpoint lock (for the time being).
	 *
	 * @param asyncResult the result to output.
	 */
	private void output(AsyncResult asyncResult) throws InterruptedException {
		if (asyncResult.isWatermark()) {
			AsyncWatermarkResult asyncWatermarkResult = asyncResult.asWatermark();

			LOG.debug("Output async watermark.");

			synchronized (checkpointLock) {
				output.emitWatermark(asyncWatermarkResult.getWatermark());
				// remove the peeked element from the async collector buffer so that it is no longer
				// checkpointed
				streamElementQueue.poll();
			}
		} else {
			AsyncCollectionResult<OUT> streamRecordResult = asyncResult.asResultCollection();

			if (streamRecordResult.hasTimestamp()) {
				timestampedCollector.setAbsoluteTimestamp(streamRecordResult.getTimestamp());
			} else {
				timestampedCollector.eraseTimestamp();
			}

			LOG.debug("Output async stream element collection result.");

			synchronized (checkpointLock) {
				try {
					Collection<OUT> resultCollection = streamRecordResult.get();

					if (resultCollection != null) {
						for (OUT result : resultCollection) {
							timestampedCollector.collect(result);
						}
					}
				} catch (Exception e) {
					operatorActions.failOperator(
						new Exception("An async function call terminated with an exception. " +
							"Failing the AsyncWaitOperator.", e));
				}

				// remove the peeked element from the async collector buffer so that it is no longer
				// checkpointed
				streamElementQueue.poll();
			}
		}
	}

	public void stop() {
		running = false;
	}
}
