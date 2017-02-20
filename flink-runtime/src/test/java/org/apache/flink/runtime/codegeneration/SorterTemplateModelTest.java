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

package org.apache.flink.runtime.codegeneration;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class SorterTemplateModelTest extends TestLogger {

	@Test
	public void testIsSortingKeyFixedSize() throws Exception {

		// key: TypeComparator
		// values: array of objects ( description, expected result )
		HashMap<TypeComparator, Object[]> testCases = new HashMap<>();

		testCases.put( TestData.getIntIntTupleComparator(), new Object[]{ "Tuple<Int,Int>", Boolean.FALSE });


		for( TypeComparator typeComp : testCases.keySet() ){
			SorterTemplateModel model = new SorterTemplateModel(typeComp);
			Object[] values = testCases.get(typeComp);
			Assert.assertEquals(
				(String)values[0],
				model.isSortingKeyFixedSize(),
				values[1]
			);
		}

	}

	@Test
	public void testGeneratedSequenceFixedByteOperators() throws Exception {


		HashMap<Integer, Integer[]> testCases = new HashMap<>();

		testCases.put(NormalizedKeySorter.DEFAULT_MAX_NORMALIZED_KEY_LEN+1,  new Integer[0]);
		// first 8 bytes is offset
		testCases.put(16, new Integer[]{8,8,8});
		testCases.put(15, new Integer[]{8,8,4,2,1});
		testCases.put(14, new Integer[]{8,8,4,2});
		testCases.put(7,  new Integer[]{8,4,2,1});
		testCases.put(3,  new Integer[]{8,2,1});
		testCases.put(1,  new Integer[]{8,1});

		for( Integer numberBytes: testCases.keySet() ) {

			SorterTemplateModel model = new SorterTemplateModel(createTypeComparatorWithCustomKeysize(numberBytes));

			Assert.assertArrayEquals(
				"Case : " + numberBytes + " bytes : ",
				testCases.get(numberBytes),
				model.getBytesOperators().toArray()
			);

		}

	}

	private TypeComparator createTypeComparatorWithCustomKeysize(final int numberBytes) {

		return new TypeComparator() {
			@Override
			public int hash(Object record) {
				return 0;
			}

			@Override
			public void setReference(Object toCompare) {

			}

			@Override
			public boolean equalToReference(Object candidate) {
				return false;
			}

			@Override
			public int compareToReference(TypeComparator referencedComparator) {
				return 0;
			}

			@Override
			public int compare(Object first, Object second) {
				return 0;
			}

			@Override
			public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
				return 0;
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
				return numberBytes;
			}

			@Override
			public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
				return false;
			}

			@Override
			public void putNormalizedKey(Object record, MemorySegment target, int offset, int numBytes) {

			}

			@Override
			public void writeWithKeyNormalization(Object record, DataOutputView target) throws IOException {

			}

			@Override
			public Object readWithKeyDenormalization(Object reuse, DataInputView source) throws IOException {
				return null;
			}

			@Override
			public boolean invertNormalizedKey() {
				return false;
			}

			@Override
			public TypeComparator duplicate() {
				return null;
			}

			@Override
			public int extractKeys(Object record, Object[] target, int index) {
				return 0;
			}

			@Override
			public TypeComparator[] getFlatComparators() {
				return new TypeComparator[0];
			}
		};
	}
}
