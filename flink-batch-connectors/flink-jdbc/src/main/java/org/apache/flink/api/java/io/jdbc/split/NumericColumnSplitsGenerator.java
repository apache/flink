/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.java.io.jdbc.split;

import org.apache.flink.api.java.io.QueryParamInputSplit;

/** 
 * 
 * This splits generator assumes that the query has a BETWEEN constraint on a numeric column.
 * The generated input splits will be of size equal to the configured fetchSize (maximum),
 * ranging from the min value up to the max 
 * 
 * */
public class NumericColumnSplitsGenerator implements JDBCInputSplitsGenerator {

	private static final long serialVersionUID = 1L;
	private long fetchSize;
	private final long min;
	private final long max;
	
	public NumericColumnSplitsGenerator(long fetchSize, long min, long max) {
		this.fetchSize = fetchSize;
		this.min = min;
		this.max = max;
	}

	@Override
	public QueryParamInputSplit[] getInputSplits(int minNumSplits) {
		double maxEelemCount = (max - min) + 1;
		int size = new Double(Math.ceil(maxEelemCount / fetchSize)).intValue();
		if(minNumSplits > size) {
			size = minNumSplits;
			fetchSize = new Double(Math.ceil(maxEelemCount / minNumSplits)).intValue();
		}
		QueryParamInputSplit[] ret = new QueryParamInputSplit[size];
		int count = 0;
		for (long i = min; i < max; i += fetchSize, count++) {
			long currentLimit = i + fetchSize - 1;
			ret[count] = new QueryParamInputSplit(count, new Long[]{i,currentLimit});
			if (currentLimit + 1 + fetchSize > max) {
				ret[count + 1] = new QueryParamInputSplit(count, new Long[]{currentLimit + 1, max});
				break;
			}
		}
		return ret;
	}

}
