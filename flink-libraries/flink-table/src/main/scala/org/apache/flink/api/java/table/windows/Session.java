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

import org.apache.flink.api.table.SessionWindow;

/**
 * Helper class for creating a session window. Session windows are ideal for cases where the
 * window boundaries need to adjust to the incoming data.In a session window it is possible to
 * have windows that start at individual points in time for each key and that end once there has
 * been a certain period of inactivity.
 */
public class Session {

	private Session() {
		// hide constructor
	}

	/**
	 * Creates a session window. Session windows are ideal for cases where the
	 * window boundaries need to adjust to the incoming data.In a session window it is possible to
	 * have windows that start at individual points in time for each key and that end once there has
	 * been a certain period of inactivity.
	 *
	 * @param gap specifies how long (as interval of milliseconds) to wait for new data before
	 *            considering a session as closed
	 * @return a session window
	 */
	public static SessionWindow over(String gap) {
		return new SessionWindow(gap);
	}
}
