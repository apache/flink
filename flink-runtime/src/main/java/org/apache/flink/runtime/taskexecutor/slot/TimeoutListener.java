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

package org.apache.flink.runtime.taskexecutor.slot;

import java.util.UUID;

/**
 * Listener for timeout events by the {@link TimerService}.
 * @param <K> Type of the timeout key
 */
public interface TimeoutListener<K> {

	/**
	 * Notify the listener about the timeout for an event identified by key. Additionally the method
	 * is called with the timeout ticket which allows to identify outdated timeout events.
	 *
	 * @param key identifying the timed out event
	 * @param ticket used to check whether the timeout is still valid
	 */
	void notifyTimeout(K key, UUID ticket);
}
