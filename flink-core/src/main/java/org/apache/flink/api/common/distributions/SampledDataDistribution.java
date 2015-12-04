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
package org.apache.flink.api.common.distributions;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * While range partition automatically, Flink sample the source data, and assign range index for each record,
 * so its data distribution is very simple, just return the bucket index as its boundary.
 */
public class SampledDataDistribution implements DataDistribution {
	@Override
	public Integer[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
		return new Integer[] { bucketNum };
	}

	@Override
	public int getNumberOfFields() {
		return 1;
	}

	@Override
	public void write(DataOutputView out) throws IOException {

	}

	@Override
	public void read(DataInputView in) throws IOException {

	}
}
