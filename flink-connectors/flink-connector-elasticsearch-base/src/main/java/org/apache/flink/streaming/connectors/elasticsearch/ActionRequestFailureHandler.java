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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.elasticsearch.action.ActionRequest;

import java.io.Serializable;

/**
 * An implementation of {@link ActionRequestFailureHandler} is provided by the user to define how failed
 * {@link ActionRequest ActionRequests} should be handled, ex. dropping them, reprocessing malformed documents, or
 * simply requesting them to be sent to Elasticsearch again if the failure is only temporary.
 *
 * <p>
 * Example:
 *
 * <pre>{@code
 *
 *	private static class ExampleActionRequestFailureHandler implements ActionRequestFailureHandler {
 *
 *		@Override
 *		boolean onFailure(ActionRequest action, Throwable failure, RequestIndexer indexer) {
 *			// this example uses Apache Commons to search for nested exceptions
 *
 *			if (ExceptionUtils.indexOfThrowable(failure, EsRejectedExecutionException.class) >= 0) {
 *				// full queue; re-add document for indexing
 *				indexer.add(action);
 *				return false;
 *			} else if (ExceptionUtils.indexOfThrowable(failure, ElasticsearchParseException.class) {
 *				// malformed document; simply drop request without failing sink
 *				return false;
 *			} else {
 *				// for all other failures, fail the sink
 *				return true;
 *			}
 *		}
 *	}
 *
 * }</pre>
 *
 * <p>
 * The above example will let the sink re-add requests that failed due to queue capacity saturation and drop requests
 * with malformed documents, without failing the sink. For all other failures, the sink will fail.
 */
public interface ActionRequestFailureHandler extends Serializable {

	/**
	 * Handle a failed {@link ActionRequest}.
	 *
	 * @param action the {@link ActionRequest} that failed due to the failure
	 * @param failure the cause of failure
	 * @param indexer request indexer to re-add the failed action, if intended to do so
	 * @return the implementation should return {@code true} if the sink should fail due to this failure, and {@code false} otherwise
	 */
	boolean onFailure(ActionRequest action, Throwable failure, RequestIndexer indexer);

}
