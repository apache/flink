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


package org.apache.flink.api.common.io.statistics;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Public;

/**
 * Interface describing the basic statistics that can be obtained from the input.
 */
@Public
public interface BaseStatistics {
	
	/**
	 * Constant indicating that the input size is unknown.
	 */
	@Experimental
	public static final long SIZE_UNKNOWN = -1;
	
	/**
	 * Constant indicating that the number of records is unknown;
	 */
	@Experimental
	public static final long NUM_RECORDS_UNKNOWN = -1;
	
	/**
	 * Constant indicating that average record width is unknown.
	 */
	@Experimental
	public static final float AVG_RECORD_BYTES_UNKNOWN = -1.0f;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the total size of the input.
	 *   
	 * @return The total size of the input, in bytes.
	 */
	@Experimental
	public long getTotalInputSize();
	
	/**
	 * Gets the number of records in the input (= base cardinality).
	 * 
	 * @return The number of records in the input.
	 */
	@Experimental
	public long getNumberOfRecords();
	
	/**
	 * Gets the average width of a record, in bytes.
	 * 
	 * @return The average width of a record in bytes.
	 */
	@Experimental
	public float getAverageRecordWidth();
}
