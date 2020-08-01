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

package org.apache.flink.table.runtime.operators.rank;

/**
 * An enumeration of rank type, usable to show how exactly generate rank number.
 */
public enum RankType {

	/**
	 * Returns a unique sequential number for each row within the partition based on the order,
	 * starting at 1 for the first row in each partition and without repeating or skipping
	 * numbers in the ranking result of each partition. If there are duplicate values within the
	 * row set, the ranking numbers will be assigned arbitrarily.
	 */
	ROW_NUMBER,

	/**
	 * Returns a unique rank number for each distinct row within the partition based on the order,
	 * starting at 1 for the first row in each partition, with the same rank for duplicate values
	 * and leaving gaps between the ranks; this gap appears in the sequence after the duplicate
	 * values.
	 */
	RANK,

	/**
	 * is similar to the RANK by generating a unique rank number for each distinct row
	 * within the partition based on the order, starting at 1 for the first row in each partition,
	 * ranking the rows with equal values with the same rank number, except that it does not skip
	 * any rank, leaving no gaps between the ranks.
	 */
	DENSE_RANK
}
