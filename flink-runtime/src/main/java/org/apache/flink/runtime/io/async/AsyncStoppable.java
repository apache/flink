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

package org.apache.flink.runtime.io.async;

/**
 * An asynchronous operation that can be stopped.
 */
public interface AsyncStoppable {

	/**
	 * Stop the operation
	 */
	void stop();

	/**
	 * Check whether the operation is stopped
	 *
	 * @return true iff operation is stopped
	 */
	boolean isStopped();

	/**
	 * Delivers Exception that might happen during {@link #stop()}
	 *
	 * @return Exception that can happen during stop
	 */
	Exception getStopException();

}
