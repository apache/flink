/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Simple utility class to work with Java's Futures.
 */
@Internal
public class FutureUtil {

	private FutureUtil() {
		throw new AssertionError();
	}

	public static <T> T runIfNotDoneAndGet(RunnableFuture<T> future) throws ExecutionException, InterruptedException {

		if (null == future) {
			return null;
		}

		if (!future.isDone()) {
			future.run();
		}

		return future.get();
	}

	public static void waitForAll(long timeoutMillis, Future<?>...futures) throws Exception {
		waitForAll(timeoutMillis, Arrays.asList(futures));
	}

	public static void waitForAll(long timeoutMillis, Collection<Future<?>> futures) throws Exception {
		long startMillis = System.currentTimeMillis();
		Set<Future<?>> futuresSet = new HashSet<>();
		futuresSet.addAll(futures);

		while (System.currentTimeMillis() < startMillis + timeoutMillis) {
			if (futuresSet.isEmpty()) {
				return;
			}
			Iterator<Future<?>> futureIterator = futuresSet.iterator();
			while (futureIterator.hasNext()) {
				Future<?> future = futureIterator.next();
				if (future.isDone()) {
					future.get();
					futureIterator.remove();
				}
			}

			Thread.sleep(10);
		}

		if (!futuresSet.isEmpty()) {
			throw new TimeoutException(String.format("Some of the futures have not finished [%s]", futuresSet));
		}
	}
}
