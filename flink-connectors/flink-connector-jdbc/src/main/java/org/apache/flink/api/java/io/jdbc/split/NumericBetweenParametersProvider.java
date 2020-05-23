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

import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;

import java.io.Serializable;

/**
 * This query parameters generator is an helper class to parameterize from/to queries on a numeric column.
 * The generated array of from/to values will be equally sized to fetchSize (apart from the last one),
 * ranging from minVal up to maxVal.
 *
 * <p>For example, if there's a table <CODE>BOOKS</CODE> with a numeric PK <CODE>id</CODE>, using a query like:
 * <PRE>
 * SELECT * FROM BOOKS WHERE id BETWEEN ? AND ?
 * </PRE>
 *
 * <p>You can take advantage of this class to automatically generate the parameters of the BETWEEN clause,
 * based on the passed constructor parameters.
 *
 * @deprecated Please use {@link JdbcNumericBetweenParametersProvider}.
 */
@Deprecated
public class NumericBetweenParametersProvider extends JdbcNumericBetweenParametersProvider implements ParameterValuesProvider {

	public NumericBetweenParametersProvider(long minVal, long maxVal) {
		super(minVal, maxVal);
	}

	public NumericBetweenParametersProvider(long fetchSize, long minVal, long maxVal) {
		super(fetchSize, minVal, maxVal);
	}

	@Override
	public NumericBetweenParametersProvider ofBatchSize(long batchSize) {
		return (NumericBetweenParametersProvider) super.ofBatchSize(batchSize);
	}

	@Override
	public NumericBetweenParametersProvider ofBatchNum(int batchNum) {
		return (NumericBetweenParametersProvider) super.ofBatchNum(batchNum);
	}

	@Override
	public Serializable[][] getParameterValues() {
		return super.getParameterValues();
	}
}
