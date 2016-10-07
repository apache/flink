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

package org.apache.flink.api.java.table.windows;

import org.apache.flink.api.table.TumblingWindow;

/**
 * Helper class for creating a tumbling window. In a tumbling window elements are assigned to
 * non-overlapping windows of a specified fixed length. For example, if you specify a tumbling
 * window of 5 minutes size, elements will be grouped in 5 minutes intervals.
 */
public class Tumble {

	private Tumble() {
		// hide constructor
	}

	/**
	 * Creates a tumbling window. In a tumbling window elements are assigned to
	 * non-overlapping windows of a specified fixed length. For example, if you specify a tumbling
	 * window of 5 minutes size, elements will be grouped in 5 minutes intervals.
	 *
	 * @param size size of the window either as number of rows or interval of milliseconds
	 * @return a tumbling window
	 */
	public static TumblingWindow over(String size) {
		return new TumblingWindow(size);
	}
}
