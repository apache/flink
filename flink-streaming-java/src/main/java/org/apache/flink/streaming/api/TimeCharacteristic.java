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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The time characteristic defines how the system determines time for time-dependent
 * order and operations that depend on time (such as time windows).
 */
@PublicEvolving
public enum TimeCharacteristic {

	/**
	 * Processing time for operators means that the operator uses the system clock of the machine
	 * to determine the current time of the data stream. Processing-time windows trigger based
	 * on wall-clock time and include whatever elements happen to have arrived at the operator at
	 * that point in time.
	 *
	 * <p>Using processing time for window operations results in general in quite non-deterministic
	 * results, because the contents of the windows depends on the speed in which elements arrive.
	 * It is, however, the cheapest method of forming windows and the method that introduces the
	 * least latency.
	 */
	ProcessingTime,

	/**
	 * Ingestion time means that the time of each individual element in the stream is determined
	 * when the element enters the Flink streaming data flow. Operations like windows group the
	 * elements based on that time, meaning that processing speed within the streaming dataflow
	 * does not affect windowing, but only the speed at which sources receive elements.
	 *
	 * <p>Ingestion time is often a good compromise between processing time and event time.
	 * It does not need and special manual form of watermark generation, and events are typically
	 * not too much out-or-order when they arrive at operators; in fact, out-of-orderness can
	 * only be introduced by streaming shuffles or split/join/union operations. The fact that
	 * elements are not very much out-of-order means that the latency increase is moderate,
	 * compared to event
	 * time.
	 */
	IngestionTime,

	/**
	 * Event time means that the time of each individual element in the stream (also called event)
	 * is determined by the event's individual custom timestamp. These timestamps either exist in
	 * the elements from before they entered the Flink streaming dataflow, or are user-assigned at
	 * the sources. The big implication of this is that it allows for elements to arrive in the
	 * sources and in all operators out of order, meaning that elements with earlier timestamps may
	 * arrive after elements with later timestamps.
	 *
	 * <p>Operators that window or order data with respect to event time must buffer data until they
	 * can be sure that all timestamps for a certain time interval have been received. This is
	 * handled by the so called "time watermarks".
	 *
	 * <p>Operations based on event time are very predictable - the result of windowing operations
	 * is typically identical no matter when the window is executed and how fast the streams
	 * operate. At the same time, the buffering and tracking of event time is also costlier than
	 * operating with processing time, and typically also introduces more latency. The amount of
	 * extra cost depends mostly on how much out of order the elements arrive, i.e., how long the
	 * time span between the arrival of early and late elements is. With respect to the
	 * "time watermarks", this means that the cost typically depends on how early or late the
	 * watermarks can be generated for their timestamp.
	 *
	 * <p>In relation to {@link #IngestionTime}, the event time is similar, but refers the the
	 * event's original time, rather than the time assigned at the data source. Practically, that
	 * means that event time has generally more meaning, but also that it takes longer to determine
	 * that all elements for a certain time have arrived.
	 */
	EventTime
}
