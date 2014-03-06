/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import java.util.Arrays;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;


public abstract class Keys<T> {


	public abstract int getNumberOfKeyFields();
	
	public boolean isEmpty() {
		return getNumberOfKeyFields() == 0;
	}
	
	public abstract boolean areCompatibale(Keys<?> other);
	
	public abstract int[] computeLogicalKeyPositions();
	
	// --------------------------------------------------------------------------------------------
	//  Specializations for field indexed / expression-based / extractor-based grouping
	// --------------------------------------------------------------------------------------------
	
	public static class FieldPositionKeys<T> extends Keys<T> {
		
		private final int[] groupingFields;
		private final TypeInformation<?>[] types;
		
		public FieldPositionKeys(int[] groupingFields, TypeInformation<T> type) {
			this(groupingFields, type, false);
		}
		
		public FieldPositionKeys(int[] groupingFields, TypeInformation<T> type, boolean allowEmpty) {
			if (!type.isTupleType()) {
				throw new InvalidProgramException("Specifying keys via field positions is only valid for tuple data types");
			}
			
			if (!allowEmpty && (groupingFields == null || groupingFields.length == 0)) {
				throw new IllegalArgumentException("The grouping fields must not be empty.");
			}
			
			TupleTypeInfo<?> tupleType = (TupleTypeInfo<?>)type;
	
			this.groupingFields = makeFields(groupingFields, (TupleTypeInfo<?>) type);
			
			types = new TypeInformation[this.groupingFields.length];
			for(int i = 0; i < this.groupingFields.length; i++) {
				types[i] = tupleType.getTypeAt(this.groupingFields[i]);
			}
			
		}

		@Override
		public int getNumberOfKeyFields() {
			return this.groupingFields.length;
		}

		@Override
		public boolean areCompatibale(Keys<?> other) {
			
			if (other instanceof FieldPositionKeys) {
				FieldPositionKeys<?> oKey = (FieldPositionKeys<?>) other;
				
				if(oKey.types.length != this.types.length) {
					return false;
				}
				for(int i=0; i<this.types.length; i++) {
					if(!this.types[i].equals(oKey.types[i])) {
						return false;
					}
				}
				return true;
				
			} else {
				return false;
			}
		}

		@Override
		public int[] computeLogicalKeyPositions() {
			return this.groupingFields;
		}
	
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class SelectorFunctionKeys<T, K> extends Keys<T> {

		private final KeySelector<T, K> keyExtractor;
		private final TypeInformation<K> keyType;
		
		public SelectorFunctionKeys(KeySelector<T, K> keyExtractor, TypeInformation<T> type) {
			this.keyExtractor = keyExtractor;
			this.keyType = TypeExtractor.getKeyExtractorType(keyExtractor);
		}

		public TypeInformation<K> getKeyType() {
			return keyType;
		}

		public KeySelector<T, K> getKeyExtractor() {
			return keyExtractor;
		}

		@Override
		public int getNumberOfKeyFields() {
			return 1;
		}

		@Override
		public boolean areCompatibale(Keys<?> other) {
			
			if (other instanceof SelectorFunctionKeys) {
				@SuppressWarnings("unchecked")
				SelectorFunctionKeys<?, K> sfk = (SelectorFunctionKeys<?, K>) other;
				
				return sfk.keyType.equals(this.keyType);
			}
			else {
				return false;
			}
		}

		@Override
		public int[] computeLogicalKeyPositions() {
			return new int[] {0};
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class ExpressionKeys<T> extends Keys<T> {

		public ExpressionKeys(String expression, TypeInformation<T> type) {
		}

		@Override
		public int getNumberOfKeyFields() {
			throw new UnsupportedOperationException("Expression keys not yet implemented");
		}

		@Override
		public boolean areCompatibale(Keys<?> other) {
			throw new UnsupportedOperationException("Expression keys not yet implemented");
		}

		@Override
		public int[] computeLogicalKeyPositions() {
			throw new UnsupportedOperationException("Expression keys not yet implemented");
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	private static int[] makeFields(int[] fields, TupleTypeInfo<?> type) {
		int inLength = type.getArity();
		
		// null parameter means all fields are considered
		if (fields == null || fields.length == 0) {
			fields = new int[inLength];
			for (int i = 0; i < inLength; i++) {
				fields[i] = i;
			}
			return fields;
		} else {
			return rangeCheckAndOrderFields(fields, inLength-1);
		}
	}
	
	private static final int[] rangeCheckAndOrderFields(int[] fields, int maxAllowedField) {
		// order
		Arrays.sort(fields);
		
		// range check and duplicate eliminate
		int i = 1, k = 0;
		int last = fields[0];
		
		if (last < 0 || last > maxAllowedField)
			throw new IllegalArgumentException("Tuple position is out of range.");
		
		for (; i < fields.length; i++) {
			if (fields[i] < 0 || i > maxAllowedField)
				throw new IllegalArgumentException("Tuple position is out of range.");
			
			if (fields[i] != last) {
				k++;
				fields[k] = fields[i];
			}
		}
		
		// check if we eliminated something
		if (k == fields.length - 1) {
			return fields;
		} else {
			return Arrays.copyOfRange(fields, 0, k);
		}
	}
}
