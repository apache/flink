/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.plugable.pactrecord;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;

/**
 * A factory for a {@link TypePairComparator} for {@link Record}. The comparator uses a subset of
 * the fields for the comparison. That subset of fields (positions and types) is read from the
 * supplied configuration.
 */
public class RecordPairComparatorFactory implements TypePairComparatorFactory<Record, Record> {
	
	private static final RecordPairComparatorFactory INSTANCE = new RecordPairComparatorFactory();
	
	/**
	 * Gets an instance of the comparator factory. The instance is shared, since the factory is a
	 * stateless class. 
	 * 
	 * @return An instance of the comparator factory.
	 */
	public static final RecordPairComparatorFactory get() {
		return INSTANCE;
	}

	@Override
	public TypePairComparator<Record, Record> createComparator12(
			TypeComparator<Record> comparator1,	TypeComparator<Record> comparator2)
	{
		if (!(comparator1 instanceof RecordComparator && comparator2 instanceof RecordComparator)) {
			throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
		}
		final RecordComparator prc1 = (RecordComparator) comparator1;
		final RecordComparator prc2 = (RecordComparator) comparator2;
		
		final int[] pos1 = prc1.getKeyPositions();
		final int[] pos2 = prc2.getKeyPositions();
		
		final Class<? extends Key<?>>[] types1 = prc1.getKeyTypes();
		final Class<? extends Key<?>>[] types2 = prc2.getKeyTypes();
		
		checkComparators(pos1, pos2, types1, types2);
		
		return new RecordPairComparator(pos1, pos2, types1);
	}

	@Override
	public TypePairComparator<Record, Record> createComparator21(
		TypeComparator<Record> comparator1,	TypeComparator<Record> comparator2)
	{
		if (!(comparator1 instanceof RecordComparator && comparator2 instanceof RecordComparator)) {
			throw new IllegalArgumentException("Cannot instantiate pair comparator from the given comparators.");
		}
		final RecordComparator prc1 = (RecordComparator) comparator1;
		final RecordComparator prc2 = (RecordComparator) comparator2;
		
		final int[] pos1 = prc1.getKeyPositions();
		final int[] pos2 = prc2.getKeyPositions();
		
		final Class<? extends Key<?>>[] types1 = prc1.getKeyTypes();
		final Class<? extends Key<?>>[] types2 = prc2.getKeyTypes();
		
		checkComparators(pos1, pos2, types1, types2);
		
		return new RecordPairComparator(pos2, pos1, types1);
	}
	
	// --------------------------------------------------------------------------------------------

	private static final void checkComparators(int[] pos1, int[] pos2, 
							Class<? extends Key<?>>[] types1, Class<? extends Key<?>>[] types2)
	{
		if (pos1.length != pos2.length || types1.length != types2.length) {
			throw new IllegalArgumentException(
				"The given pair of RecordComparators does not operate on the same number of fields.");
		}
		for (int i = 0; i < types1.length; i++) {
			if (!types1[i].equals(types2[i])) {
				throw new IllegalArgumentException(
				"The given pair of RecordComparators does not operates on different data types.");
			}
		}
	}
}
