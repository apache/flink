/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources.parquet;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;

import java.util.Arrays;

/**
 * {@link TypeInformation} for {@link VectorizedColumnBatch}.
 */

public class VectorizedColumnBatchTypeInfo extends TypeInformation<VectorizedColumnBatch> {
	private static final long serialVersionUID = -666939301459922848L;

	private final TypeInformation<?>[] types;
	private final int totalFields;
	private final String[] filedNames;

	public VectorizedColumnBatchTypeInfo(String[] filedNames, TypeInformation<?>... types) {
		this.types = types;
		this.filedNames = filedNames;
		int fieldCounter = 0;

		for (TypeInformation<?> type : types) {
			fieldCounter += type.getTotalFields();
		}

		totalFields = fieldCounter;
	}

	public TypeInformation<?>[] getTypes() {
		return types;
	}

	public String[] getFiledNames() {
		return filedNames;
	}

	@Override
	public boolean isSortKeyType() {
		return false;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return types.length;
	}

	@Override
	public int getTotalFields() {
		return totalFields;
	}

	@Override
	public Class<VectorizedColumnBatch> getTypeClass() {
		return VectorizedColumnBatch.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<VectorizedColumnBatch> createSerializer(ExecutionConfig executionConfig) {
		throw new UnsupportedOperationException("Unsupported now!");
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof VectorizedColumnBatchTypeInfo;
	}

	@Override
	public String toString() {
		return "VectorizedColumnBatchTypeInfo{" + "types=" + Arrays.toString(types) + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof VectorizedColumnBatchTypeInfo)) {
			return false;
		}

		VectorizedColumnBatchTypeInfo that = (VectorizedColumnBatchTypeInfo) o;

		// Probably incorrect - comparing Object[] arrays with Arrays.equals
		return Arrays.equals(types, that.types);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(types);
	}
}
