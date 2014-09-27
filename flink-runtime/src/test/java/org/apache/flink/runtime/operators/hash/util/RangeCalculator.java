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


package org.apache.flink.runtime.operators.hash.util;

/**
 * Interface for defining which hash code will be placed in which bucket
 * 
 * 
 */
public interface RangeCalculator {

	/**
	 * Calculates the number of the bucket in which the value should be placed
	 * in
	 * 
	 * @param hash
	 *            The hash of the value
	 * @return The number of the bucket
	 */
	public int getBucket(int hash);

	/**
	 * Get the total bucket count
	 * 
	 * @return The total bucket count
	 */
	public int getBucketCount();

}
