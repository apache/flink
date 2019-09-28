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

package org.apache.flink.runtime.messages.webmonitor;

/**
 * This message requests an overview of the status, such as how many TaskManagers
 * are currently connected, how many slots are available, how many are free, ...
 * The response to this message is a {@link ClusterOverview} message.
 */
public class RequestStatusOverview implements InfoMessage {

	private static final long serialVersionUID = 3052933564788843275L;
	
	// ------------------------------------------------------------------------
	
	private static final RequestStatusOverview INSTANCE = new RequestStatusOverview();

	public static RequestStatusOverview getInstance() {
		return INSTANCE;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return RequestStatusOverview.class.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj != null && obj.getClass() == RequestStatusOverview.class;
	}

	@Override
	public String toString() {
		return RequestStatusOverview.class.getSimpleName();
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * No external instantiation
	 */
	private RequestStatusOverview() {}

	/**
	 * Preserve the singleton property by returning the singleton instance
	 */
	private Object readResolve() {
		return INSTANCE;
	}
}
