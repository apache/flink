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

package org.apache.flink.runtime.concurrent.impl;

import akka.dispatch.Futures;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import scala.concurrent.Promise;
import scala.concurrent.Promise$;

import java.util.concurrent.CancellationException;

/**
 * Implementation of {@link CompletableFuture} which is backed by {@link Promise}.
 *
 * @param <T> type of the future's value
 */
public class FlinkCompletableFuture<T> extends FlinkFuture<T> implements CompletableFuture<T> {

	private final Promise<T> promise;

	public FlinkCompletableFuture() {
		promise = Futures.promise();
		scalaFuture = promise.future();
	}

	private FlinkCompletableFuture(T value) {
		promise = Promise$.MODULE$.successful(value);
		scalaFuture = promise.future();
	}

	private FlinkCompletableFuture(Throwable t) {
		promise = Promise$.MODULE$.failed(t);
		scalaFuture = promise.future();
	}

	@Override
	public boolean complete(T value) {
		try {
			promise.success(value);

			return true;
		} catch (IllegalStateException e) {
			return false;
		}
	}

	@Override
	public boolean completeExceptionally(Throwable t) {
		try {
			if (t == null) {
				promise.failure(new NullPointerException("Throwable was null."));
			} else {
				promise.failure(t);
			}

			return true;
		} catch (IllegalStateException e) {
			return false;
		}
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return completeExceptionally(new CancellationException("Future has been canceled."));
	}

	public static <T> FlinkCompletableFuture<T> completed(T value) {
		return new FlinkCompletableFuture<>(value);
	}

	public static <T> FlinkCompletableFuture<T> completedExceptionally(Throwable t) {
		return new FlinkCompletableFuture<>(t);
	}
}
