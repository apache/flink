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

package org.apache.flink.runtime.webmonitor;

/**
 * Interface for web monitors. Defines life-cycle methods and properties.
 */
public interface WebMonitor {

	/**
	 * Starts the web monitor.
	 *
	 * @throws Exception This method may forward exceptions, if it cannot bring up the web monitor.
	 */
	void start() throws Exception;

	/**
	 * Stops the web server.
	 *
	 * @throws Exception This method may forward exceptions, if it cannot properly stop the web monitor.
	 */
	void stop() throws Exception;

	/**
	 * Gets the port that the web server actually binds to. If port 0 was given in
	 * the configuration, then a random free port will be picked. This method can
	 * be used to determine this port.
	 *
	 * @return The port where the web server is listening, or -1, if no server is running.
	 */
	int getServerPort();

	/**
	 * Returns the REST address of this WebMonitor.
	 *
	 * @return REST address of this WebMonitor
	 */
	String getRestAddress();
}
