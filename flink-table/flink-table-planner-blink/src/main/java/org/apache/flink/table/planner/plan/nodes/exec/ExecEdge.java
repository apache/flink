/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.util.Preconditions;

/**
 * The representation of an edge connecting two {@link ExecNode}.
 */
public class ExecEdge {

	private final RequiredShuffle requiredShuffle;
	private final DamBehavior damBehavior;
	// the priority of this edge read by the target node
	// the smaller the integer, the higher the priority
	// same integer indicates the same priority
	private final int priority;

	private ExecEdge(RequiredShuffle requiredShuffle, DamBehavior damBehavior, int priority) {
		this.requiredShuffle = requiredShuffle;
		this.damBehavior = damBehavior;
		this.priority = priority;
	}

	public RequiredShuffle getRequiredShuffle() {
		return requiredShuffle;
	}

	public DamBehavior getDamBehavior() {
		return damBehavior;
	}

	public int getPriority() {
		return priority;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder of the {@link ExecEdge}.
	 */
	public static class Builder {
		private RequiredShuffle requiredShuffle;
		private DamBehavior damBehavior;
		private int priority;

		private Builder() {
			this.requiredShuffle = RequiredShuffle.unknown();
			this.damBehavior = DamBehavior.PIPELINED;
			this.priority = 0;
		}

		public Builder requiredShuffle(RequiredShuffle requiredShuffle) {
			this.requiredShuffle = requiredShuffle;
			return this;
		}

		public Builder damBehavior(DamBehavior damBehavior) {
			this.damBehavior = damBehavior;
			return this;
		}

		public Builder priority(int priority) {
			this.priority = priority;
			return this;
		}

		public ExecEdge build() {
			return new ExecEdge(requiredShuffle, damBehavior, priority);
		}
	}

	/**
	 * The required shuffle for records when passing this edge.
	 */
	public static class RequiredShuffle {

		private final ShuffleType type;
		private final int[] keys;

		private RequiredShuffle(ShuffleType type) {
			this(type, new int[0]);
		}

		private RequiredShuffle(ShuffleType type, int[] keys) {
			this.type = type;
			this.keys = keys;
		}

		public ShuffleType getType() {
			return type;
		}

		public int[] getKeys() {
			return keys;
		}

		public static RequiredShuffle any() {
			return new RequiredShuffle(ShuffleType.ANY);
		}

		public static RequiredShuffle hash(int[] keys) {
			Preconditions.checkArgument(keys.length > 0, "Hash keys must no be empty.");
			return new RequiredShuffle(ShuffleType.HASH, keys);
		}

		public static RequiredShuffle broadcast() {
			return new RequiredShuffle(ShuffleType.BROADCAST);
		}

		public static RequiredShuffle singleton() {
			return new RequiredShuffle(ShuffleType.SINGLETON);
		}

		public static RequiredShuffle unknown() {
			return new RequiredShuffle(ShuffleType.UNKNOWN);
		}
	}

	/**
	 * Enumeration which describes the shuffle type for records when passing this edge.
	 */
	public enum ShuffleType {

		/**
		 * Any type of shuffle is OK when passing through this edge.
		 */
		ANY,

		/**
		 * Records are shuffle by hash when passing through this edge.
		 */
		HASH,

		/**
		 * Full records are provided for each parallelism of the target node.
		 */
		BROADCAST,

		/**
		 * The parallelism of the target node must be 1.
		 */
		SINGLETON,

		/**
		 * Unknown shuffle type, will be filled out in the future.
		 */
		UNKNOWN
	}

	/**
	 * Enumeration which describes how an output record from the source node
	 * may trigger the output of the target node.
	 */
	public enum DamBehavior {

		/**
		 * Constant indicating that some or all output records from the source
		 * will immediately trigger one or more output records of the target.
		 */
		PIPELINED,

		/**
		 * Constant indicating that only the last output record from the source
		 * will immediately trigger one or more output records of the target.
		 */
		END_INPUT,

		/**
		 * Constant indicating that all output records from the source
		 * will not trigger output records of the target.
		 */
		BLOCKING;

		public boolean stricterOrEqual(DamBehavior o) {
			return ordinal() >= o.ordinal();
		}
	}
}
