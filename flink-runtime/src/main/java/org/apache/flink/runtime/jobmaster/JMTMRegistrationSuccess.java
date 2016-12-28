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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.util.Preconditions;

public class JMTMRegistrationSuccess extends RegistrationResponse.Success {
	private static final long serialVersionUID = -3528383155961318929L;

	private final ResourceID resourceID;
	private final int blobPort;

	public JMTMRegistrationSuccess(ResourceID resourceID, int blobPort) {
		Preconditions.checkArgument(0 < blobPort && 65536 > blobPort, "The blob port has to be 0 < blobPort < 65536.");

		this.resourceID = Preconditions.checkNotNull(resourceID);
		this.blobPort = blobPort;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	public int getBlobPort() {
		return blobPort;
	}
}
