/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.shipping;

/**
 * Enumeration defining the different shipping types of the output, such as local forward, re-partitioning by hash,
 * or re-partitioning by range.
 * 
 * @author Stephan Ewen
 */
public enum ShipStrategyType
{
	NONE(false, false),
	FORWARD(false, false),
	PARTITION_HASH(true, true),
	PARTITION_LOCAL_HASH(false, true),
	PARTITION_RANGE(true, true),
	PARTITION_LOCAL_RANGE(false, true),
	BROADCAST(true, false);
	
	// --------------------------------------------------------------------------------------------
	
	private final boolean isNetwork;
	
	private final boolean requiresComparator;
	
	private ShipStrategyType(boolean network, boolean requiresComparator) {
		this.isNetwork = network;
		this.requiresComparator = requiresComparator;
	}
	
	public boolean isNetworkStrategy() {
		return this.isNetwork;
	}
	
	public boolean requiresComparator() {
		return this.requiresComparator;
	}
}
