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

package org.apache.flink.orc.nohive;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcColumnarRowSplitReader;
import org.apache.flink.orc.OrcFilters;
import org.apache.flink.orc.OrcSplitReader;
import org.apache.flink.orc.nohive.shim.OrcNoHiveShim;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.flink.orc.OrcSplitReaderUtil.convertToOrcTypeWithPart;
import static org.apache.flink.orc.OrcSplitReaderUtil.getNonPartNames;
import static org.apache.flink.orc.OrcSplitReaderUtil.getSelectedOrcFields;
import static org.apache.flink.orc.nohive.vector.AbstractOrcNoHiveVector.createFlinkVector;
import static org.apache.flink.orc.nohive.vector.AbstractOrcNoHiveVector.createFlinkVectorFromConstant;

/**
 * Util for generating {@link OrcSplitReader}.
 */
public class OrcNoHiveSplitReaderUtil {

	/**
	 * Util for generating partitioned {@link OrcColumnarRowSplitReader}.
	 */
	public static OrcColumnarRowSplitReader<VectorizedRowBatch> genPartColumnarRowReader(
			Configuration conf,
			String[] fullFieldNames,
			DataType[] fullFieldTypes,
			Map<String, Object> partitionSpec,
			int[] selectedFields,
			List<OrcFilters.Predicate> conjunctPredicates,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {

		List<String> nonPartNames = getNonPartNames(fullFieldNames, partitionSpec);

		int[] selectedOrcFields = getSelectedOrcFields(fullFieldNames, selectedFields, nonPartNames);

		OrcColumnarRowSplitReader.ColumnBatchGenerator<VectorizedRowBatch> gen = (VectorizedRowBatch rowBatch) -> {
			// create and initialize the row batch
			ColumnVector[] vectors = new ColumnVector[selectedFields.length];
			for (int i = 0; i < vectors.length; i++) {
				String name = fullFieldNames[selectedFields[i]];
				LogicalType type = fullFieldTypes[selectedFields[i]].getLogicalType();
				vectors[i] = partitionSpec.containsKey(name) ?
						createFlinkVectorFromConstant(type, partitionSpec.get(name), batchSize) :
						createFlinkVector(rowBatch.cols[nonPartNames.indexOf(name)]);
			}
			return new VectorizedColumnBatch(vectors);
		};

		return new OrcColumnarRowSplitReader<>(
				new OrcNoHiveShim(),
				conf,
				convertToOrcTypeWithPart(fullFieldNames, fullFieldTypes, partitionSpec.keySet()),
				selectedOrcFields,
				gen,
				conjunctPredicates,
				batchSize,
				path,
				splitStart,
				splitLength);
	}

}
