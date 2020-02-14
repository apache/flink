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

package org.apache.flink.orc;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcColumnarRowSplitReader.ColumnBatchGenerator;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.orc.vector.AbstractOrcColumnVector.createVector;
import static org.apache.flink.orc.vector.AbstractOrcColumnVector.createVectorFromConstant;

/**
 * Util for generating {@link OrcSplitReader}.
 */
public class OrcSplitReaderUtil {

	/**
	 * Util for generating partitioned {@link OrcColumnarRowSplitReader}.
	 */
	public static OrcColumnarRowSplitReader genPartColumnarRowReader(
			String hiveVersion,
			Configuration conf,
			String[] fullFieldNames,
			DataType[] fullFieldTypes,
			Map<String, Object> partitionSpec,
			int[] selectedFields,
			List<OrcSplitReader.Predicate> conjunctPredicates,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {

		List<String> nonPartNames = Arrays.stream(fullFieldNames)
				.filter(n -> !partitionSpec.containsKey(n))
				.collect(Collectors.toList());

		int[] selectedOrcFields = Arrays.stream(selectedFields)
				.mapToObj(i -> fullFieldNames[i])
				.filter(nonPartNames::contains)
				.mapToInt(nonPartNames::indexOf)
				.toArray();

		ColumnBatchGenerator gen = rowBatch -> {
			// create and initialize the row batch
			ColumnVector[] vectors = new ColumnVector[selectedFields.length];
			for (int i = 0; i < vectors.length; i++) {
				String name = fullFieldNames[selectedFields[i]];
				LogicalType type = fullFieldTypes[selectedFields[i]].getLogicalType();
				vectors[i] = partitionSpec.containsKey(name) ?
						createVectorFromConstant(type, partitionSpec.get(name), batchSize) :
						createVector(rowBatch.cols[nonPartNames.indexOf(name)]);
			}
			return new VectorizedColumnBatch(vectors);
		};

		return new OrcColumnarRowSplitReader(
				OrcShim.createShim(hiveVersion),
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

	private static TypeDescription convertToOrcTypeWithPart(
			String[] fullFieldNames,
			DataType[] fullFieldTypes,
			Collection<String> partitionKeys) {
		List<String> fullNameList = Arrays.asList(fullFieldNames);
		String[] orcNames = fullNameList.stream()
				.filter(n -> !partitionKeys.contains(n))
				.toArray(String[]::new);
		LogicalType[] orcTypes = Arrays.stream(orcNames)
				.mapToInt(fullNameList::indexOf)
				.mapToObj(i -> fullFieldTypes[i].getLogicalType())
				.toArray(LogicalType[]::new);
		return logicalTypeToOrcType(RowType.of(
				orcTypes,
				orcNames));
	}

	/**
	 * See {@code org.apache.flink.table.catalog.hive.util.HiveTypeUtil}.
	 */
	static TypeDescription logicalTypeToOrcType(LogicalType type) {
		type = type.copy(true);
		switch (type.getTypeRoot()) {
			case CHAR:
				return TypeDescription.createChar().withMaxLength(((CharType) type).getLength());
			case VARCHAR:
				return TypeDescription.createVarchar().withMaxLength(((VarCharType) type).getLength());
			case BOOLEAN:
				return TypeDescription.createBoolean();
			case VARBINARY:
				if (type.equals(DataTypes.BYTES().getLogicalType())) {
					return TypeDescription.createBinary();
				} else {
					throw new UnsupportedOperationException(
							"Not support other binary type: " + type);
				}
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return TypeDescription.createDecimal()
						.withPrecision(decimalType.getPrecision())
						.withScale(decimalType.getScale());
			case TINYINT:
				return TypeDescription.createByte();
			case SMALLINT:
				return TypeDescription.createShort();
			case INTEGER:
				return TypeDescription.createInt();
			case BIGINT:
				return TypeDescription.createLong();
			case FLOAT:
				return TypeDescription.createFloat();
			case DOUBLE:
				return TypeDescription.createDouble();
			case DATE:
				return TypeDescription.createDate();
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return TypeDescription.createTimestamp();
			case ARRAY:
				ArrayType arrayType = (ArrayType) type;
				return TypeDescription.createList(logicalTypeToOrcType(arrayType.getElementType()));
			case MAP:
				MapType mapType = (MapType) type;
				return TypeDescription.createMap(
						logicalTypeToOrcType(mapType.getKeyType()),
						logicalTypeToOrcType(mapType.getValueType()));
			case ROW:
				RowType rowType = (RowType) type;
				TypeDescription struct = TypeDescription.createStruct();
				for (int i = 0; i < rowType.getFieldCount(); i++) {
					struct.addField(
							rowType.getFieldNames().get(i),
							logicalTypeToOrcType(rowType.getChildren().get(i)));
				}
				return struct;
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}
}
