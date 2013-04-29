/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.plugable.arrayrecord;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypePairComparator;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;

/**
 * A factory for a {@link TypePairComparator} for {@link PactRecord}. The comparator uses a subset of
 * the fields for the comparison. That subset of fields (positions and types) is read from the
 * supplied configuration.
 */
public class ArrayRecordPairComparatorFactory implements TypePairComparatorFactory<Value[], Value[]>
{
	private static final ArrayRecordPairComparatorFactory INSTANCE = new ArrayRecordPairComparatorFactory();
	
	/**
	 * Gets an instance of the comparator factory. The instance is shared, since the factory is a
	 * stateless class. 
	 * 
	 * @return An instance of the comparator factory.
	 */
	public static final ArrayRecordPairComparatorFactory get() {
		return INSTANCE;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory#createComparator12(eu.stratosphere.pact.common.generic.types.TypeComparator, eu.stratosphere.pact.common.generic.types.TypeComparator)
	 */
	@Override
	public TypePairComparator<Value[], Value[]> createComparator12(
			TypeComparator<Value[]> comparator1, TypeComparator<Value[]> comparator2)
	{
		if (!(comparator1 instanceof ArrayRecordComparator && comparator2 instanceof ArrayRecordComparator)) {
			throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
		}
		final ArrayRecordComparator prc1 = (ArrayRecordComparator) comparator1;
		final ArrayRecordComparator prc2 = (ArrayRecordComparator) comparator2;
		
		final int[] pos1 = prc1.getKeyPositions();
		final int[] pos2 = prc2.getKeyPositions();
		
		final Class<? extends Key>[] types1 = prc1.getKeyTypes();
		final Class<? extends Key>[] types2 = prc2.getKeyTypes();
		
		checkComparators(pos1, pos2, types1, types2);
		
		return new ArrayRecordPairComparator(pos1, pos2, types1);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory#createComparator21(eu.stratosphere.pact.common.generic.types.TypeComparator, eu.stratosphere.pact.common.generic.types.TypeComparator)
	 */
	@Override
	public TypePairComparator<Value[], Value[]> createComparator21(
		TypeComparator<Value[]> comparator1, TypeComparator<Value[]> comparator2)
	{
		if (!(comparator1 instanceof ArrayRecordComparator && comparator2 instanceof ArrayRecordComparator)) {
			throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
		}
		final ArrayRecordComparator prc1 = (ArrayRecordComparator) comparator1;
		final ArrayRecordComparator prc2 = (ArrayRecordComparator) comparator2;
		
		final int[] pos1 = prc1.getKeyPositions();
		final int[] pos2 = prc2.getKeyPositions();
		
		final Class<? extends Key>[] types1 = prc1.getKeyTypes();
		final Class<? extends Key>[] types2 = prc2.getKeyTypes();
		
		checkComparators(pos1, pos2, types1, types2);
		
		return new ArrayRecordPairComparator(pos2, pos1, types1);
	}
	
	// --------------------------------------------------------------------------------------------

	private static final void checkComparators(int[] pos1, int[] pos2, 
							Class<? extends Key>[] types1, Class<? extends Key>[] types2)
	{
		if (pos1.length != pos2.length || types1.length != types2.length) {
			throw new IllegalArgumentException(
				"The given pair of PactRecordComparators does not operate on the same number of fields.");
		}
		for (int i = 0; i < types1.length; i++) {
			if (!types1[i].equals(types2[i])) {
				throw new IllegalArgumentException(
				"The given pair of PactRecordComparators does not operates on different data types.");
			}
		}
	}
}
