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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CompletableFuture;

/**
 * Wrapper for an initializing JobManagerRunner.
 */
public class InitializingJobManagerRunner {
	private final Logger log = LoggerFactory.getLogger(InitializingJobManagerRunner.class);

	private final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = new CompletableFuture<>();

	private final Thread initializationRunner;

	private final Object lock = new Object();

	@GuardedBy("lock")
	private CompletableFuture<Acknowledge> cancellationFuture;

	public InitializingJobManagerRunner(SupplierWithException<JobManagerRunner, JobInitializationException> initializationRunnable, JobID jobID) {
		String threadName = "jobmanager-initialization-" + jobID;
		this.initializationRunner = new Thread(() -> {
			log.debug("Starting JobManager initialization thread: {}", threadName);
			try {
				runInitialization(initializationRunnable);
			} catch (Throwable threadError) {
				log.warn("Unexpected error in thread '{}'", threadName, threadError);
			} finally {
				log.debug("Stopping JobManager initialization thread: {}", threadName);
			}
		}, threadName);
		this.initializationRunner.setDaemon(true); // this should not block the JVM from shutting down
		this.initializationRunner.start();
	}

	private void runInitialization(SupplierWithException<JobManagerRunner, JobInitializationException> initializationRunnable) {
		try {
			JobManagerRunner runner = initializationRunnable.get();
			synchronized (lock) {
				if (cancellationFuture == null) {
					jobManagerRunnerFuture.complete(runner);
				} else {
					// jobMaster has finished despite the interruption. Cancel job.
					FutureUtils.forward(runner.getJobMasterGateway().thenCompose(gateway -> gateway.cancel(RpcUtils.INF_TIMEOUT)),
						cancellationFuture);
					jobManagerRunnerFuture.complete(runner);
				}
			}
		} catch (JobInitializationException throwable) {
			synchronized (lock) {
				if (cancellationFuture == null) {
					jobManagerRunnerFuture.completeExceptionally(throwable);
				} else {
					// an exception related to an interruption is expected here.
					log.debug("Cancellation finished with (expected) exception", throwable);
					jobManagerRunnerFuture.completeExceptionally(new JobInitializationCancelledException("Job initialization has been cancelled", throwable));
					cancellationFuture.complete(Acknowledge.get());
				}
			}
		}
	}

	public CompletableFuture<Acknowledge> cancelInitialization() {
		synchronized (lock) {
			initializationRunner.interrupt();
			cancellationFuture = new CompletableFuture<>();
			return cancellationFuture;
		}
	}

	public CompletableFuture<JobManagerRunner> getJobManagerRunnerFuture() {
		return jobManagerRunnerFuture;
	}
}
