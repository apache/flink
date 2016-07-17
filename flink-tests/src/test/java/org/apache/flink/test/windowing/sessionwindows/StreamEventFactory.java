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
 * Factory that produces events of keyed session streams
 *
 * @param <K>
 * @param <E>
 */
public interface StreamEventFactory<K, E> {

	/**
	 * @param key             session key
	 * @param sessionId       session id
	 * @param eventId         event id w.r.t. the session
	 * @param eventTimestamp  event time
	 * @param globalWatermark current value of the global watermark
	 * @param timing          indicator for lateness
	 * @return event for an keyed event stream
	 */
	E createEvent(K key,
	              int sessionId,
	              int eventId,
	              long eventTimestamp,
	              long globalWatermark,
	              SessionEventGeneratorImpl.Timing timing);
}
