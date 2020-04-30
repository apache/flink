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


package org.apache.flink.runtime.operators;

/**
 * Enumeration for the different dam behaviors of an algorithm or a driver
 * strategy. The dam behavior describes whether records pass through the
 * algorithm (no dam), whether all records are collected before the first
 * is returned (full dam) or whether a certain large amount is collected before
 * the algorithm returns records. 
 */
public enum DamBehavior {
	
	/**
	 * Constant indicating that the algorithm does not come with any form of dam
	 * and records pass through in a pipelined fashion.
	 */
	PIPELINED,
	
	/**
	 * Constant indicating that the algorithm materialized (some) records, but may
	 * return records before all records are read.
	 */
	MATERIALIZING,
	
	/**
	 * Constant indicating that the algorithm collects all records before returning any.
	 */
	FULL_DAM;
	
	/**
	 * Checks whether this enumeration represents some form of materialization,
	 * either with a full dam or without.
	 * 
	 * @return True, if this enumeration constant represents a materializing behavior,
	 *         false otherwise.
	 */
	public boolean isMaterializing() {
		return this != PIPELINED;
	}
}
