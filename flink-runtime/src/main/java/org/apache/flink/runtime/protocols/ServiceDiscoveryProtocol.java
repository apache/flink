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

package org.apache.flink.runtime.protocols;

import org.apache.flink.core.protocols.VersionedProtocol;

/**
 * The service discovery protocols enables different components of the Flink distributed runtime to query and discover
 * the network location auxiliary services.
 */
public interface ServiceDiscoveryProtocol extends VersionedProtocol {

	/**
	 * Returns the network port of the job manager's BLOB server.
	 * 
	 * @return the port of the job manager's BLOB server or <code>-1</code> if the job manager does not run a BLOB
	 *         server
	 */
	int getBlobServerPort();
}
