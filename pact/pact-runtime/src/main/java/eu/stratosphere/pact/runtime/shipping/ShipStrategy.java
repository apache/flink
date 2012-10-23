/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.pact.common.util.FieldList;

/**
 * Enumeration defining the different shipping types of the output, such as local forward, re-partitioning by hash,
 * or re-partitioning by range.
 */
public abstract class ShipStrategy {
	
	public enum ShipStrategyType {
		FORWARD,
		PARTITION_HASH,
		PARTITION_LOCAL_HASH,
		PARTITION_RANGE,
		PARTITION_LOCAL_RANGE,
		BROADCAST,
		SFR,
		NONE
	}
	
	private ShipStrategyType type;
	
	private ShipStrategy(ShipStrategyType type) {
		this.type = type;
	}
	
	public ShipStrategyType type() {
		return this.type;
	}
	
	public String name() {
		return this.type.name();
	}
	
	// ------------------ SHIP STRATEGIES ---------------
	
	public static class ForwardSS extends ShipStrategy {
	
		public ForwardSS() { super(ShipStrategyType.FORWARD); }
	}
	
	public static class BroadcastSS extends ShipStrategy {
		
		public BroadcastSS() { super(ShipStrategyType.BROADCAST); } 
	}
	
	public static class SFRSS extends ShipStrategy {
		
		public SFRSS() { super(ShipStrategyType.SFR); }
	}
	
	public static class NoneSS extends ShipStrategy {
		
		public NoneSS() { super(ShipStrategyType.NONE); }
	}
	
	public static abstract class PartitionShipStrategy extends ShipStrategy {
		
		private FieldList partitionFields;
		
		private PartitionShipStrategy(ShipStrategyType type, FieldList partitionFields) { 
			super(type);
			this.partitionFields = partitionFields;
		}
		
		public FieldList getPartitionFields() {
			return this.partitionFields;
		}
		
	}
	
	public static class PartitionHashSS extends PartitionShipStrategy {
		
		public PartitionHashSS(FieldList partitionFields) { 
			super(ShipStrategyType.PARTITION_HASH, partitionFields);
		}
		
	}

	public static class PartitionLocalHashSS extends PartitionShipStrategy {
		
		public PartitionLocalHashSS(FieldList partitionFields) { 
			super(ShipStrategyType.PARTITION_LOCAL_HASH, partitionFields);
		}
		
	}
	
	public static class PartitionRangeSS extends PartitionShipStrategy {
		
		public PartitionRangeSS(FieldList partitionFields) { 
			super(ShipStrategyType.PARTITION_RANGE, partitionFields);
		}
		
	}
	
	public static class PartitionLocalRangeSS extends PartitionShipStrategy {
		
		public PartitionLocalRangeSS(FieldList partitionFields) { 
			super(ShipStrategyType.PARTITION_LOCAL_RANGE, partitionFields);
		}
		
	}
	
}