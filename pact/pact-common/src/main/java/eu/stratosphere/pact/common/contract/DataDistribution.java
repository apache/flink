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

package eu.stratosphere.pact.common.contract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.PactRecord;

public interface DataDistribution 
{
	/**
	 * Returns the i'th bucket's upper bound, given that the distribution is to be
	 * split into {@code totalBuckets} buckets.
	 * <p>
	 * Assuming <i>n</i> buckets, let {@code B_i} be the result from calling {@code getBucketBoundary(i, n)},
	 * then the distribution will partition the data domain in the following fashion:
	 * 
	 * <pre>
	 * (-inf, B_1] (B_1, B_2] ... (B_n-2, B_n-1] (B_n-1, inf)
	 * </pre>
	 * <p>
	 * Note: The last bucket's upper bound is actually discarded by many algorithms.
	 * The last bucket is assumed to hold all values <i>v</i> such that
	 * {@code v &gt; getBucketBoundary(n-1, n)}, where <i>n</i> is the number of buckets.
	 * 
	 * @param bucketNum The number of the bucket for which to get the upper bound.
	 * @param totalNumBuckets The number of buckets to split the data into.
	 * 
	 * @return
	 */
	public PactRecord getBucketBoundary(int bucketNum, int totalNumBuckets); 
	
	public void write(DataOutput out) throws IOException;

	public void read(DataInput in) throws IOException;
}
