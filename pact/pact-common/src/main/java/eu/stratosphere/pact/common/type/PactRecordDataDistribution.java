/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.common.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.contract.DataDistribution;

/**
 * Data distribution for the PactRecord record type.
 * 
 */
public class PactRecordDataDistribution implements DataDistribution<PactRecord> {

	private PactRecord[] boundaryRecords;  // records that mark the upper boundary of all buckets 
	
	/**
	 * Default constructor for deserialization.
	 */
	public PactRecordDataDistribution() {
		// do nothing
	}
	
	@SuppressWarnings("unchecked")
	public PactRecordDataDistribution(int[] keyPositions, Key[][] boundaryKeys) {
		
		Class<? extends Key>[] keyTypes = new Class[keyPositions.length];
		
		int numBoundaryKeys = 0;
		// check boundary keys for consistency
		for(Key[] boundaryKey : boundaryKeys) {
			
			// check for consistent number of keys
			if(numBoundaryKeys == 0) {
				if(keyPositions.length != boundaryKey.length) {
					throw new IllegalArgumentException("Length of key positions and boundary keys do not match.");
				}
				numBoundaryKeys = boundaryKey.length;
			} else if (numBoundaryKeys != boundaryKey.length) {
				throw new IllegalArgumentException("All boundaries need the same number of keys.");
			}
			
			// check for consistent key types
			for(int i = 0; i < boundaryKey.length; i++) {
				if(keyTypes[i] == null) {
					keyTypes[i] = boundaryKey[i].getClass();
				} else if (! boundaryKey[i].getClass().equals(keyTypes[i])) {
					throw new IllegalArgumentException("Boundary keys do not match the ordering key types.");
				}
			}
		}
		
		// create records from keys
		this.boundaryRecords = new PactRecord[boundaryKeys.length];
		
		for(int i = 0; i < boundaryKeys.length; i++) {
			this.boundaryRecords[i] = new PactRecord();
			for(int j = 0; j < boundaryKeys[i].length; j++) {
				this.boundaryRecords[i].setField(keyPositions[j], boundaryKeys[i][j]);
			}
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// write number of boundary records
		out.writeInt(this.boundaryRecords.length);
		// write out boundary records
		for(int i = 0; i < this.boundaryRecords.length; i++) {
			boundaryRecords[i].write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		// read number of boundary records
		final int numRecords = in.readInt();
		boundaryRecords = new PactRecord[numRecords];
		// read records
		for(int i = 0; i < numRecords; i++) {
			boundaryRecords[i] = new PactRecord();
			boundaryRecords[i].read(in);
		}
	}

	@Override
	public PactRecord getBucketBoundary(int bucketNum, int totalNumBuckets) {

		// check validity of arguments
		if(bucketNum < 0) {
			throw new IllegalArgumentException("Requested bucket must be greater than or equal to 0.");
		} else if(bucketNum >= (totalNumBuckets - 1)) {
			throw new IllegalArgumentException("Request bucket must be smaller than the total number of buckets minus 1.");
		}
		if(totalNumBuckets < 1) {
			throw new IllegalArgumentException("Total number of bucket must be larger than 0.");
		}
		
		final int maxNumBuckets = this.boundaryRecords.length + 1;
		
		// check if max number of buckets is equal to or an even multiple of the requested number of buckets
		if((maxNumBuckets % totalNumBuckets) == 0) {
			// easy case, just use each n-th boundary
			final int n = maxNumBuckets / totalNumBuckets;
			final int bucketId = bucketNum * n + (n -  1); 
			
			return boundaryRecords[bucketId];
		} else {
			throw new IllegalArgumentException("Interpolation of bucket boundaries currently not supported. " +
					"Please use an even divider of the maximum possible buckets (here: "+maxNumBuckets+") as totalBuckets.");
			// TODO: might be relaxed if much more boundary records are available than requested
		}
	}
	
}
