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

package org.apache.flink.streaming.api.windowing.helper;

import java.io.Serializable;

/**
 * Interface for getting a timestamp from a custom value. Used in window
 * reduces. In order to work properly, the timestamps must be non-decreasing.
 *
 * @param <T>
 *            Type of the value to create the timestamp from.
 */
public interface Timestamp<T> extends Serializable {

	/**
	 * Values
	 * 
	 * @param value
	 *            The value to create the timestamp from
	 * @return The timestamp
	 */
	public long getTimestamp(T value);
}
