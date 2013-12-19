/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.io.statistics;

/**
 * Interface describing the basic statistics that can be obtained from the input.
 */
public interface BaseStatistics {
	
	/**
	 * Constant indicating that the input size is unknown.
	 */
	public static final long SIZE_UNKNOWN = -1;
	
	/**
	 * Constant indicating that the number of records is unknown;
	 */
	public static final long NUM_RECORDS_UNKNOWN = -1;
	
	/**
	 * Constant indicating that average record width is unknown.
	 */
	public static final float AVG_RECORD_BYTES_UNKNOWN = -1.0f;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the total size of the input.
	 *   
	 * @return The total size of the input, in bytes.
	 */
	public long getTotalInputSize();
	
	/**
	 * Gets the number of records in the input (= base cardinality).
	 * 
	 * @return The number of records in the input.
	 */
	public long getNumberOfRecords();
	
	/**
	 * Gets the average width of a record, in bytes.
	 * 
	 * @return The average width of a record in bytes.
	 */
	public float getAverageRecordWidth();
}
