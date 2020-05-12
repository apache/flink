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

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.io.Serializable;

/**
 * An implementation of {@link CassandraFailureHandler} is provided by the user to define how
 * {@link Throwable Throwable} should be handled, e.g. dropping them if the failure is only temporary.
 *
 * <p>Example:
 *
 * <pre>{@code
 *
 * 	private static class ExampleFailureHandler implements CassandraFailureHandler {
 *
 * 		@Override
 * 		void onFailure(Throwable failure) throws IOException {
 * 			if (ExceptionUtils.findThrowable(failure, WriteTimeoutException.class).isPresent()) {
 * 				// drop exception
 * 			} else {
 * 				// for all other failures, fail the sink;
 * 				// here the failure is simply rethrown, but users can also choose to throw custom exceptions
 * 				throw failure;
 * 			}
 * 		}
 * 	}
 *
 * }</pre>
 *
 * <p>The above example will let the sink ignore the WriteTimeoutException, without failing the sink. For all other
 * failures, the sink will fail.
 */
@PublicEvolving
public interface CassandraFailureHandler extends Serializable {

	/**
	 * Handle a failed {@link Throwable}.
	 *
	 * @param failure the cause of failure
	 * @throws IOException if the sink should fail on this failure, the implementation should rethrow the throwable or a custom one
	 */
	void onFailure(Throwable failure) throws IOException;

}
