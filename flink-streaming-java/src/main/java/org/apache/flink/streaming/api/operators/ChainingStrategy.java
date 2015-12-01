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

package org.apache.flink.streaming.api.operators;


/**
 * Defines the chaining scheme for the operator.
 * By default {@link #ALWAYS} is used, which means operators will be eagerly chained whenever possible.
 */
public enum ChainingStrategy {

	/**
	 * Chaining will happen even if chaining is disabled on the execution environment.
	 * This should only be used by system-level operators, not operators implemented by users.
	 */
	FORCE_ALWAYS,

	/** 
	 * Operators will be eagerly chained whenever possible, for
	 * maximal performance. It is generally a good practice to allow maximal
	 * chaining and increase operator parallelism
	 */
	ALWAYS,

	/**
	 * The operator will not be chained to the preceding or succeeding operators.
	 */
	NEVER,
	
	
	HEAD
}
