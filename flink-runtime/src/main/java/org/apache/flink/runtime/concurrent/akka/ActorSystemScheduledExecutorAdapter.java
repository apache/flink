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

package org.apache.flink.runtime.concurrent.akka;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledFutureTask;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import akka.actor.Cancellable;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

/**
 * Adapter to use a {@link ActorSystem} as a {@link ScheduledExecutor}.
 */
public final class ActorSystemScheduledExecutorAdapter implements ScheduledExecutor {

	private final ActorSystem actorSystem;

	public ActorSystemScheduledExecutorAdapter(ActorSystem actorSystem) {
		this.actorSystem = Preconditions.checkNotNull(actorSystem, "rpcService");
	}

	@Override
	@Nonnull
	public ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
		ScheduledFutureTask<Void> scheduledFutureTask = new ScheduledFutureTask<>(command, delay, unit);

		Cancellable cancellable = internalSchedule(scheduledFutureTask, delay, unit);

		scheduledFutureTask.setCancellable(new CancellableAdapter(cancellable));

		return scheduledFutureTask;
	}

	@Override
	@Nonnull
	public <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
		ScheduledFutureTask<V> scheduledFutureTask = new ScheduledFutureTask<>(callable, delay, unit);

		Cancellable cancellable = internalSchedule(scheduledFutureTask, delay, unit);

		scheduledFutureTask.setCancellable(new CancellableAdapter(cancellable));

		return scheduledFutureTask;
	}

	@Override
	@Nonnull
	public ScheduledFuture<?> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay, long period, @Nonnull TimeUnit unit) {
		ScheduledFutureTask<Void> scheduledFutureTask = new ScheduledFutureTask<>(
			command,
			initialDelay,
			period,
			unit);

		Cancellable cancellable = actorSystem.scheduler().schedule(
			new FiniteDuration(initialDelay, unit),
			new FiniteDuration(period, unit),
			scheduledFutureTask,
			actorSystem.dispatcher());

		scheduledFutureTask.setCancellable(new CancellableAdapter(cancellable));

		return scheduledFutureTask;
	}

	@Override
	@Nonnull
	public ScheduledFuture<?> scheduleWithFixedDelay(@Nonnull Runnable command, long initialDelay, long delay, @Nonnull TimeUnit unit) {
		ScheduledFutureTask<Void> scheduledFutureTask = new ScheduledFutureTask<>(
			command,
			initialDelay,
			delay,
			unit,
			(runnable, delayTime, timeUnit) ->
				new CancellableAdapter(internalSchedule(runnable, delayTime, timeUnit)));

		Cancellable cancellable = internalSchedule(scheduledFutureTask, initialDelay, unit);

		scheduledFutureTask.setCancellable(new CancellableAdapter(cancellable));

		return scheduledFutureTask;
	}

	@Override
	public void execute(@Nonnull Runnable command) {
		actorSystem.dispatcher().execute(command);
	}

	private Cancellable internalSchedule(Runnable runnable, long delay, TimeUnit unit) {
		return actorSystem.scheduler().scheduleOnce(
			new FiniteDuration(delay, unit),
			runnable,
			actorSystem.dispatcher());
	}

	private static class CancellableAdapter implements ScheduledFutureTask.Cancellable {

		private final Cancellable cancellable;

		private CancellableAdapter(Cancellable cancellable) {
			this.cancellable = cancellable;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return cancellable.cancel();
		}

		@Override
		public boolean isCancelled() {
			return cancellable.isCancelled();
		}
	}
}
