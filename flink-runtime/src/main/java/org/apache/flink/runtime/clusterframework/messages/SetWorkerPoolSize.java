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

package org.apache.flink.runtime.clusterframework.messages;

import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

/**
 * Message sent to the resource master actor to adjust the designated number of
 * workers it maintains.
 */
public class SetWorkerPoolSize implements RequiresLeaderSessionID, java.io.Serializable{

	private static final long serialVersionUID = -335911350781207609L;
	
	private final int numberOfWorkers;

	public SetWorkerPoolSize(int numberOfWorkers) {
		if (numberOfWorkers < 0) {
			throw new IllegalArgumentException("Number of workers must not be negative.");
		}
		this.numberOfWorkers = numberOfWorkers;
	}

	// ------------------------------------------------------------------------
	
	public int numberOfWorkers() {
		return numberOfWorkers;
	}
	
	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return numberOfWorkers;
	}

	@Override
	public boolean equals(Object obj) {
		return obj != null && obj.getClass() == SetWorkerPoolSize.class && 
					this.numberOfWorkers == ((SetWorkerPoolSize) obj).numberOfWorkers;
	}

	@Override
	public String toString() {
		return "SetWorkerPoolSize (" + numberOfWorkers + ')';
	}
}
