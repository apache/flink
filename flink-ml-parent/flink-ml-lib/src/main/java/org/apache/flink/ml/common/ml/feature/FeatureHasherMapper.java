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

package org.apache.flink.ml.common.ml.feature;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.mapper.Mapper;
import org.apache.flink.ml.common.utils.OutputColsHelper;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.ml.feature.FeatureHasherParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;

import org.apache.commons.lang3.ArrayUtils;

import java.util.TreeMap;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * Projects a number of categorical or numerical features into a feature vector of a specified dimension. It's done by
 * using MurMurHash3 to map the features to indices of the result vector and accumulate the corresponding value.
 *
 * <p>For categorical feature: the string to hash is "colName=value", here colName is the colName of the feature, the
 * value is the feature value. The corresponding value is set 1.0.
 *
 * <p>For numerical feature: the string to hash is "colName", the colName of the feature and use the numeric value as
 * the corresponding value.
 *
 * <p>The numerical feature and categorical feature can be determined automatically. You can also change the numerical
 * features to categorical features by determine the CATEGORICAL_COLS parameter.
 *
 * <p>(https://en.wikipedia.org/wiki/Feature_hashing)
 */
public class FeatureHasherMapper extends Mapper {
	private int[] numericColIndexes, categoricalColIndexes;
	private OutputColsHelper outputColsHelper;
	private static final HashFunction HASH = murmur3_32(0);
	private int numFeature;
	private String[] colNames;

	public FeatureHasherMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		String[] selectedCols = this.params.get(FeatureHasherParams.SELECTED_COLS);
		String[] categoricalCols = TableUtil.getCategoricalCols(
			dataSchema,
			selectedCols,
			params.contains(FeatureHasherParams.CATEGORICAL_COLS) ?
				params.get(FeatureHasherParams.CATEGORICAL_COLS) : null
		);
		String[] numericCols = ArrayUtils.removeElements(selectedCols, categoricalCols);
		colNames = dataSchema.getFieldNames();

		numericColIndexes = TableUtil.findColIndices(colNames, numericCols);
		categoricalColIndexes = TableUtil.findColIndices(colNames, categoricalCols);

		outputColsHelper = new OutputColsHelper(
			dataSchema,
			this.params.get(FeatureHasherParams.OUTPUT_COL),
			VectorTypes.VECTOR,
			this.params.get(FeatureHasherParams.RESERVED_COLS)
		);

		numFeature = this.params.get(FeatureHasherParams.NUM_FEATURES);
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	/**
	 * Projects a number of categorical or numerical features into a feature vector of a specified dimension.
	 *
	 * @param row the input Row type data
	 * @return the output row.
	 */
	@Override
	public Row map(Row row) {
		TreeMap<Integer, Double> feature = new TreeMap<>();
		for (int key : numericColIndexes) {
			if (null != row.getField(key)) {
				double value = ((Number) row.getField(key)).doubleValue();
				String colName = colNames[key];
				updateMap(colName, value, feature, numFeature);
			}
		}
		for (int key : categoricalColIndexes) {
			if (null != row.getField(key)) {
				String colName = colNames[key];
				updateMap(colName + "=" + row.getField(key).toString(), 1.0, feature, numFeature);
			}
		}

		return outputColsHelper.getResultRow(row, Row.of(new SparseVector(numFeature, feature)));
	}

	/**
	 * Update the treeMap which saves the key-value pair of the final vector, use the hash value of the string as key
	 * and the accumulate the corresponding value.
	 *
	 * @param s     the string to hash
	 * @param value the accumulated value
	 */
	private static void updateMap(String s, double value, TreeMap<Integer, Double> feature, int numFeature) {
		int hashValue = Math.abs(HASH.hashUnencodedChars(s).asInt());

		int index = Math.floorMod(hashValue, numFeature);
		if (feature.containsKey(index)) {
			feature.put(index, feature.get(index) + value);
		} else {
			feature.put(index, value);
		}
	}
}
