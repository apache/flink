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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.runtime.tasks.InputSelector.InputSelection;

/**
 * The interface Input fetcher.
 */
interface InputFetcher {

	/**
	 * Do some setting up work.
	 *
	 * @throws Exception the exception
	 */
	void setup() throws Exception;

	/**
	 * Fetch and process.
	 *
	 * @return true if the fetcher wants to process continuously
	 * @throws Exception the exception
	 */
	boolean fetchAndProcess() throws Exception;

	/**
	 * Check the fetcher is finished or not.
	 *
	 * @return true if the fetcher is finished, return false otherwise
	 */
	boolean isFinished();

	/**
	 * Check is there any data available currently.
	 *
	 * @return the true if there is more data available
	 */
	boolean moreAvailable();

	/**
	 * Cleanup.
	 *
	 * @throws Exception the exception
	 */
	void cleanup() throws Exception;

	/**
	 * Cancel.
	 *
	 * @throws Exception the exception
	 */
	void cancel() throws Exception;

	/**
	 * Get the input selection of this input fetcher.
	 *
	 * @return the input selection
	 */
	InputSelection getInputSelection();

	/**
	 * Register the available listener.
	 *
	 * @param listener the listener
	 */
	void registerAvailableListener(InputFetcherAvailableListener listener);

	/**
	 * The interface Input fetcher available listener.
	 */
	interface InputFetcherAvailableListener {

		/**
		 * Notify input fetcher is available.
		 *
		 * @param inputFetcher the input fetcher
		 */
		void notifyInputFetcherAvailable(InputFetcher inputFetcher);
	}
}
