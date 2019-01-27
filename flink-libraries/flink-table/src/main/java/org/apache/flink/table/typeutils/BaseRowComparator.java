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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.GeneratedRecordComparator;
import org.apache.flink.table.codegen.SortCodeGenerator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.plan.util.SortUtil$;
import org.apache.flink.table.runtime.sort.RecordComparator;

import org.codehaus.commons.compiler.CompileException;

import java.io.IOException;
import java.util.Arrays;

import scala.Tuple2;

/**
 * For sort by nested row field.
 */
public class BaseRowComparator extends TypeComparator<BaseRow> {

	private GeneratedRecordComparator genComparator;
	private final Tuple2<TypeComparator<?>[], TypeSerializer<?>[]> comAndSers;
	private final boolean order;
	private RecordComparator comparator;
	private final TypeComparator<?>[] comparators = new TypeComparator[] {this};

	public BaseRowComparator(TypeInformation<?>[] types, boolean order) {
		this.order = order;
		int[] keys = new int[types.length];
		boolean[] orders = new boolean[types.length];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = i;
			orders[i] = true;
		}
		this.comAndSers = TypeUtils.flattenComparatorAndSerializer(
				keys.length, keys, orders, types);
		this.genComparator = new SortCodeGenerator(
				keys,
				Arrays.stream(types).map(TypeConverters::createInternalTypeFromTypeInfo).toArray(InternalType[]::new),
				comAndSers._1,
				orders,
				SortUtil$.MODULE$.getNullDefaultOrders(orders)
		).generateRecordComparator("BaseRowComparator");
	}

	public RecordComparator getComparator()
			throws CompileException, IllegalAccessException, InstantiationException {
		if (comparator == null) {
			comparator = (RecordComparator) CodeGenUtils.compile(
					// currentThread must be user class loader.
					Thread.currentThread().getContextClassLoader(),
					genComparator.name(), genComparator.code()).newInstance();
			genComparator = null;
			comparator.init(comAndSers._2, comAndSers._1);
		}
		return comparator;
	}

	@Override
	public int hash(BaseRow record) {
		throw new RuntimeException();
	}

	@Override
	public void setReference(BaseRow toCompare) {
		throw new RuntimeException();
	}

	@Override
	public boolean equalToReference(BaseRow candidate) {
		throw new RuntimeException();
	}

	@Override
	public int compareToReference(TypeComparator<BaseRow> referencedComparator) {
		throw new RuntimeException();
	}

	@Override
	public int compare(BaseRow first, BaseRow second) {
		try {
			int cmp = getComparator().compare(first, second);
			return order ? cmp : -cmp;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int compareSerialized(DataInputView firstSource,
			DataInputView secondSource) throws IOException {
		throw new RuntimeException();
	}

	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 0;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return false;
	}

	@Override
	public void putNormalizedKey(BaseRow record, MemorySegment target, int offset, int numBytes) {
		throw new RuntimeException();
	}

	@Override
	public void writeWithKeyNormalization(BaseRow record,
			DataOutputView target) throws IOException {
		throw new RuntimeException();
	}

	@Override
	public BaseRow readWithKeyDenormalization(BaseRow reuse,
			DataInputView source) throws IOException {
		throw new RuntimeException();
	}

	@Override
	public boolean invertNormalizedKey() {
		throw new RuntimeException();
	}

	@Override
	public TypeComparator<BaseRow> duplicate() {
		throw new RuntimeException();
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		throw new RuntimeException();
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return comparators;
	}
}
