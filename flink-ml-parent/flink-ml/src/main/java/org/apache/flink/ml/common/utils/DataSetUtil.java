/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;

/**
 * Util for DataSet.
 */
public class DataSetUtil {
	/**
	 * Count number of records in the dataset.
	 *
	 * @return a dataset of one record, recording the number of records of [[dataset]]
	 */
	public static <T> DataSet <Long> count(DataSet <T> dataSet) {
		return dataSet
			.mapPartition(new MapPartitionFunction <T, Long>() {
				@Override
				public void mapPartition(Iterable <T> values, Collector <Long> out) throws Exception {
					long cnt = 0L;
					for (T v : values) {
						cnt++;
					}
					out.collect(cnt);
				}
			})
			.name("count_dataset")
			.returns(Types.LONG)
			.reduce(new ReduceFunction <Long>() {
				@Override
				public Long reduce(Long value1, Long value2) throws Exception {
					return value1 + value2;
				}
			});
	}

	/**
	 * Returns an empty dataset of the same type as [[dataSet]].
	 */
	public static <T> DataSet <T> empty(DataSet <T> dataSet) {
		return dataSet
			.mapPartition(new MapPartitionFunction <T, T>() {
				@Override
				public void mapPartition(Iterable <T> values, Collector <T> out) throws Exception {
				}
			})
			.returns(dataSet.getType());
	}
}
