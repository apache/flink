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

package org.apache.flink.test.distribution;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * The class is used to do the tests of range partition with customed data distribution.
 */
public class TestDataDist implements DataDistribution {

	private int dim;

	public TestDataDist() {}

	/**
	 * Constructor of the customized distribution for range partition.
	 * @param dim the number of the fields.
	 */
	public TestDataDist(int dim) {
		this.dim = dim;
	}

	public int getParallelism() {
		return 3;
	}

	@Override
	public Object[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
		if (dim == 1) {
			return new Integer[]{(bucketNum + 1) * 7};
		}
		return new Integer[]{(bucketNum + 1) * 7, (bucketNum) * 2 + 3};
	}

	@Override
	public int getNumberOfFields() {
		return this.dim;
	}
	
	@Override
	public TypeInformation[] getKeyTypes() {
		return new TypeInformation[]{TypeExtractor.getForClass(Integer.class)};
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.dim);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.dim = in.readInt();
	}
}
