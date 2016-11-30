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
package org.apache.flink.api.java.io.jdbc.split;

import java.io.Serializable;

/** 
 * 
 * This query generator assumes that the query to parameterize contains a BETWEEN constraint on a numeric column.
 * The generated query set will be of size equal to the configured fetchSize (apart the last one range),
 * ranging from the min value up to the max.
 * 
 * For example, if there's a table <CODE>BOOKS</CODE> with a numeric PK <CODE>id</CODE>, using a query like:
 * <PRE>
 *   SELECT * FROM BOOKS WHERE id BETWEEN ? AND ?
 * </PRE>
 *
 * you can use this class to automatically generate the parameters of the BETWEEN clause,
 * based on the passed constructor parameters.
 * 
 * */
public class NumericBetweenParametersProvider implements ParameterValuesProvider {

	private long fetchSize;
	private final long min;
	private final long max;
	
	public NumericBetweenParametersProvider(long fetchSize, long min, long max) {
		this.fetchSize = fetchSize;
		this.min = min;
		this.max = max;
	}

	@Override
	public Serializable[][] getParameterValues(){
		double maxElemCount = (max - min) + 1;
		int size = new Double(Math.ceil(maxElemCount / fetchSize)).intValue();
		Serializable[][] parameters = new Serializable[size][2];
		int count = 0;
		for (long i = min; i < max; i += fetchSize, count++) {
			long currentLimit = i + fetchSize - 1;
			parameters[count] = new Long[]{i,currentLimit};
			if (currentLimit + 1 + fetchSize > max) {
				parameters[count + 1] = new Long[]{currentLimit + 1, max};
				break;
			}
		}
		return parameters;
	}
	
}
