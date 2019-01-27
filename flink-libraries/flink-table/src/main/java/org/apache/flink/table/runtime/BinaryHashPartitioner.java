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

package org.apache.flink.table.runtime;

import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.CodeGeneratorContext;
import org.apache.flink.table.codegen.GeneratedHashFunc;
import org.apache.flink.table.codegen.HashCodeGenerator;
import org.apache.flink.table.codegen.HashFunc;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.MathUtils;

import java.util.Arrays;

/**
 * Hash partitioner for {@link BinaryRow}.
 */
public class BinaryHashPartitioner extends StreamPartitioner<BaseRow> {

	private GeneratedHashFunc genHashFunc;
	private final String[] hashFieldNames;

	private transient HashFunc hashFunc;

	public BinaryHashPartitioner(BaseRowTypeInfo info, int[] hashFields) {
		this.genHashFunc = HashCodeGenerator.generateRowHash(
				CodeGeneratorContext.apply(new TableConfig(), false),
				TypeConverters.createInternalTypeFromTypeInfo(info),
				"HashPartitioner", hashFields);
		this.hashFieldNames = new String[hashFields.length];
		String[] fieldNames = info.getFieldNames();
		for (int i = 0; i < hashFields.length; i++) {
			hashFieldNames[i] = fieldNames[hashFields[i]];
		}
	}

	@Override
	public StreamPartitioner<BaseRow> copy() {
		return this;
	}

	@Override
	public int selectChannel(StreamRecord<BaseRow> record, int numChannels) {
		return MathUtils.murmurHash(
				getHashFunc().apply(record.getValue())) % numChannels;
	}

	private HashFunc getHashFunc() {
		if (hashFunc == null) {
			try {
				hashFunc = (HashFunc) CodeGenUtils.compile(
						// currentThread must be user class loader.
						Thread.currentThread().getContextClassLoader(),
						genHashFunc.name(), genHashFunc.code()).newInstance();
				genHashFunc = null;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return hashFunc;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BinaryHashPartitioner that = (BinaryHashPartitioner) o;
		return Arrays.equals(hashFieldNames, that.hashFieldNames);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(hashFieldNames);
	}

	@Override
	public String toString() {
		return "HASH" + Arrays.toString(hashFieldNames);
	}
}
