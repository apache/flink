/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple utility to retry failed SQL commands for a predefined number of times
 * before declaring failure. The retrier waits (randomly) between 2 retries.
 *
 */
public final class SQLRetrier implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(SQLRetrier.class);
	private static final Random rnd = new Random();

	private static final int SLEEP_TIME = 10;

	private SQLRetrier() {

	}

	/**
	 * Tries to run the given {@link Callable} the predefined number of times
	 * before throwing an {@link IOException}. This method will only retries
	 * calls ending in {@link SQLException}. Other exceptions will cause a
	 * {@link RuntimeException}.
	 * 
	 * @param callable
	 *            The callable to be retried.
	 * @param numRetries
	 *            Max number of retries before throwing an {@link IOException}.
	 * @throws IOException
	 *             The wrapped {@link SQLException}.
	 */
	public static <X> X retry(Callable<X> callable, int numRetries) throws IOException {
		return retry(callable, numRetries, SLEEP_TIME);
	}

	/**
	 * Tries to run the given {@link Callable} the predefined number of times
	 * before throwing an {@link IOException}. This method will only retries
	 * calls ending in {@link SQLException}. Other exceptions will cause a
	 * {@link RuntimeException}.
	 * 
	 * @param callable
	 *            The callable to be retried.
	 * @param numRetries
	 *            Max number of retries before throwing an {@link IOException}.
	 * @param sleep
	 *            The retrier will wait a random number of msecs between 1 and
	 *            sleep.
	 * @return The result of the {@link Callable#call()}.
	 * @throws IOException
	 *             The wrapped {@link SQLException}.
	 */
	public static <X> X retry(Callable<X> callable, int numRetries, int sleep) throws IOException {
		int numtries = 0;
		while (true) {
			try {
				return callable.call();
			} catch (SQLException e) {
				handleSQLException(e, ++numtries, numRetries, sleep);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Tries to run the given {@link Callable} the predefined number of times
	 * before throwing an {@link IOException}. This method will only retries
	 * calls ending in {@link SQLException}. Other exceptions will cause a
	 * {@link RuntimeException}.
	 * 
	 * Additionally the user can supply a second callable which will be executed
	 * every time the first callable throws a {@link SQLException}.
	 * 
	 * @param callable
	 *            The callable to be retried.
	 * @param onException
	 *            The callable to be executed when an {@link SQLException} was
	 *            encountered. Exceptions thrown during this call are ignored.
	 * @param numRetries
	 *            Max number of retries before throwing an {@link IOException}.
	 * @param sleep
	 *            The retrier will wait a random number of msecs between 1 and
	 *            sleep.
	 * @return The result of the {@link Callable#call()}.
	 * @throws IOException
	 *             The wrapped {@link SQLException}.
	 */
	public static <X, Y> X retry(Callable<X> callable, Callable<Y> onException, int numRetries, int sleep)
			throws IOException {
		int numtries = 0;
		while (true) {
			try {
				return callable.call();
			} catch (SQLException se) {
				try {
					onException.call();
				} catch (Exception e) {
					// Exceptions thrown in this call will be ignored
				}
				handleSQLException(se, ++numtries, numRetries, sleep);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	/**
	 * Tries to run the given {@link Callable} the predefined number of times
	 * before throwing an {@link IOException}. This method will only retries
	 * calls ending in {@link SQLException}. Other exceptions will cause a
	 * {@link RuntimeException}.
	 * 
	 * Additionally the user can supply a second callable which will be executed
	 * every time the first callable throws a {@link SQLException}.
	 * 
	 * @param callable
	 *            The callable to be retried.
	 * @param onException
	 *            The callable to be executed when an {@link SQLException} was
	 *            encountered. Exceptions thrown during this call are ignored.
	 * @param numRetries
	 *            Max number of retries before throwing an {@link IOException}.
	 * @return The result of the {@link Callable#call()}.
	 * @throws IOException
	 *             The wrapped {@link SQLException}.
	 */
	public static <X, Y> X retry(Callable<X> callable, Callable<Y> onException, int numRetries)
			throws IOException {
		return retry(callable, onException, numRetries, SLEEP_TIME);
	}

	private static void handleSQLException(SQLException e, int numTries, int maxRetries, int sleep) throws IOException {
		if (numTries < maxRetries) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Error while executing SQL statement: {}\nRetrying...",
						e.getMessage());
			}
			try {
				Thread.sleep(numTries * rnd.nextInt(sleep));
			} catch (InterruptedException ie) {
				throw new RuntimeException("Thread has been interrupted.");
			}
		} else {
			throw new IOException(
					"Could not execute SQL statement after " + maxRetries + " retries.", e);
		}
	}
}
