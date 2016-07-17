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

package org.apache.flink.test.windowing.sessionwindows;

/**
 * @param <K> session key type
 * @param <E> session event type
 */
public interface EventGenerator<K, E> {

	/**
	 * Only call this method if hasMoreEvents() is true and canProduceEventAtWatermark(...) is true.
	 *
	 * @param globalWatermark
	 * @return a generated event
	 */
	E generateEvent(long globalWatermark);

	/**
	 * @param globalWatermark
	 * @return true if the generator can produce events for the provided global watermark
	 */
	boolean canProduceEventAtWatermark(long globalWatermark);

	/**
	 * @return true more events can been produced
	 */
	boolean hasMoreEvents();

	/**
	 * @return the watermark that tracks this generator's progress
	 */
	long getLocalWatermark();

	/**
	 * @param globalWatermark
	 * @return a successor for this generator if hasMoreEvents() is false
	 */
	EventGenerator<K, E> getNextGenerator(long globalWatermark);

	/**
	 * @return key for the generated events
	 */
	K getKey();
}
