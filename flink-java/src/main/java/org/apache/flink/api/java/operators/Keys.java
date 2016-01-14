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

package org.apache.flink.api.java.operators;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.KeyRemovingMapper;
import org.apache.flink.api.java.operators.translation.TwoKeyExtractingMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import com.google.common.base.Preconditions;

public abstract class Keys<T> {

	public abstract int getNumberOfKeyFields();

	public abstract int[] computeLogicalKeyPositions();

	protected abstract TypeInformation<?>[] getKeyFieldTypes();

	public abstract <E> void validateCustomPartitioner(Partitioner<E> partitioner, TypeInformation<E> typeInfo);

	public boolean isEmpty() {
		return getNumberOfKeyFields() == 0;
	}

	/**
	 * Check if two sets of keys are compatible to each other (matching types, key counts)
	 */
	public boolean areCompatible(Keys<?> other) throws IncompatibleKeysException {

		TypeInformation<?>[] thisKeyFieldTypes = this.getKeyFieldTypes();
		TypeInformation<?>[] otherKeyFieldTypes = other.getKeyFieldTypes();

		if (thisKeyFieldTypes.length != otherKeyFieldTypes.length) {
			throw new IncompatibleKeysException(IncompatibleKeysException.SIZE_MISMATCH_MESSAGE);
		} else {
			for (int i = 0; i < thisKeyFieldTypes.length; i++) {
				if (!thisKeyFieldTypes[i].equals(otherKeyFieldTypes[i])) {
					throw new IncompatibleKeysException(thisKeyFieldTypes[i], otherKeyFieldTypes[i] );
				}
			}
		}
		return true;
	}

	// --------------------------------------------------------------------------------------------
	//  Specializations for expression-based / extractor-based grouping
	// --------------------------------------------------------------------------------------------
	
	
	public static class SelectorFunctionKeys<T, K> extends Keys<T> {

		private final KeySelector<T, K> keyExtractor;
		private final TypeInformation<T> inputType;
		private final TypeInformation<K> keyType;
		private final List<FlatFieldDescriptor> keyFields;

		public SelectorFunctionKeys(KeySelector<T, K> keyExtractor, TypeInformation<T> inputType, TypeInformation<K> keyType) {

			if (keyExtractor == null) {
				throw new NullPointerException("Key extractor must not be null.");
			}
			if (keyType == null) {
				throw new NullPointerException("Key type must not be null.");
			}
			if (!keyType.isKeyType()) {
				throw new InvalidProgramException("Return type "+keyType+" of KeySelector "+keyExtractor.getClass()+" is not a valid key type");
			}

			this.keyExtractor = keyExtractor;
			this.inputType = inputType;
			this.keyType = keyType;

			if (keyType instanceof CompositeType) {
				this.keyFields = ((CompositeType<T>)keyType).getFlatFields(ExpressionKeys.SELECT_ALL_CHAR);
			}
			else {
				this.keyFields = new ArrayList<>(1);
				this.keyFields.add(new FlatFieldDescriptor(0, keyType));
			}
		}

		public TypeInformation<K> getKeyType() {
			return keyType;
		}

		public TypeInformation<T> getInputType() {
			return inputType;
		}

		public KeySelector<T, K> getKeyExtractor() {
			return keyExtractor;
		}

		@Override
		public int getNumberOfKeyFields() {
			return keyFields.size();
		}

		@Override
		public int[] computeLogicalKeyPositions() {
			int[] logicalKeys = new int[keyFields.size()];
			for (int i = 0; i < keyFields.size(); i++) {
				logicalKeys[i] = keyFields.get(i).getPosition();
			}
			return logicalKeys;
		}

		@Override
		protected TypeInformation<?>[] getKeyFieldTypes() {
			TypeInformation<?>[] fieldTypes = new TypeInformation[keyFields.size()];
			for (int i = 0; i < keyFields.size(); i++) {
				fieldTypes[i] = keyFields.get(i).getType();
			}
			return fieldTypes;
		}
		
		@Override
		public <E> void validateCustomPartitioner(Partitioner<E> partitioner, TypeInformation<E> typeInfo) {

			if (keyFields.size() != 1) {
				throw new InvalidProgramException("Custom partitioners can only be used with keys that have one key field.");
			}
			
			if (typeInfo == null) {
				// try to extract key type from partitioner
				try {
					typeInfo = TypeExtractor.getPartitionerTypes(partitioner);
				}
				catch (Throwable t) {
					// best effort check, so we ignore exceptions
				}
			}

			// only check if type is known and not a generic type
			if (typeInfo != null && !(typeInfo instanceof GenericTypeInfo)) {
				// check equality of key and partitioner type
				if (!keyType.equals(typeInfo)) {
					throw new InvalidProgramException("The partitioner is incompatible with the key type. "
						+ "Partitioner type: " + typeInfo + " , key type: " + keyType);
				}
			}
		}

		@SuppressWarnings("unchecked")
		public static <T, K> Operator<Tuple2<K, T>> appendKeyExtractor(
			Operator<T> input,
			SelectorFunctionKeys<T, K> key)
		{

			TypeInformation<T> inputType = key.getInputType();
			TypeInformation<Tuple2<K, T>> typeInfoWithKey = createTypeWithKey(key);
			KeyExtractingMapper<T, K> extractor = new KeyExtractingMapper(key.getKeyExtractor());

			MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>> mapper =
				new MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>>(
					extractor,
					new UnaryOperatorInformation(inputType, typeInfoWithKey),
					"Key Extractor"
				);

			mapper.setInput(input);
			mapper.setParallelism(input.getParallelism());

			return mapper;
		}

		@SuppressWarnings("unchecked")
		public static <T, K1, K2> Operator<Tuple3<K1, K2, T>> appendKeyExtractor(
			Operator<T> input,
			SelectorFunctionKeys<T, K1> key1,
			SelectorFunctionKeys<T, K2> key2)
		{

			TypeInformation<T> inputType = key1.getInputType();
			TypeInformation<Tuple3<K1, K2, T>> typeInfoWithKey = createTypeWithKey(key1, key2);
			TwoKeyExtractingMapper<T, K1, K2> extractor =
				new TwoKeyExtractingMapper<>(key1.getKeyExtractor(), key2.getKeyExtractor());

			MapOperatorBase<T, Tuple3<K1, K2, T>, MapFunction<T, Tuple3<K1, K2, T>>> mapper =
				new MapOperatorBase<T, Tuple3<K1, K2, T>, MapFunction<T, Tuple3<K1, K2, T>>>(
					extractor,
					new UnaryOperatorInformation<>(inputType, typeInfoWithKey),
					"Key Extractor"
				);

			mapper.setInput(input);
			mapper.setParallelism(input.getParallelism());

			return mapper;
		}

		public static <T, K> org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> appendKeyRemover(
			Operator<Tuple2<K, T>> inputWithKey,
			SelectorFunctionKeys<T, K> key)
		{

			TypeInformation<T> inputType = key.getInputType();
			TypeInformation<Tuple2<K, T>> typeInfoWithKey = createTypeWithKey(key);

			MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>> mapper =
				new MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>>(
					new KeyRemovingMapper<T, K>(),
					new UnaryOperatorInformation<>(typeInfoWithKey, inputType),
					"Key Remover"
				);
			mapper.setInput(inputWithKey);
			mapper.setParallelism(inputWithKey.getParallelism());

			return mapper;
		}

		public static <T, K> TypeInformation<Tuple2<K, T>> createTypeWithKey(
			SelectorFunctionKeys<T, K> key)
		{
			return new TupleTypeInfo<>(key.getKeyType(), key.getInputType());
		}

		public static <T, K1, K2> TypeInformation<Tuple3<K1, K2, T>> createTypeWithKey(
			SelectorFunctionKeys<T, K1> key1,
			SelectorFunctionKeys<T, K2> key2)
		{
			return new TupleTypeInfo<>(key1.getKeyType(), key2.getKeyType(), key1.getInputType());
		}

		@Override
		public String toString() {
			return "Key function (Type: " + keyType + ")";
		}
	}
	
	
	/**
	 * Represents (nested) field access through string and integer-based keys
	 */
	public static class ExpressionKeys<T> extends Keys<T> {
		
		public static final String SELECT_ALL_CHAR = "*";
		public static final String SELECT_ALL_CHAR_SCALA = "_";
		
		// Flattened fields representing keys fields
		private List<FlatFieldDescriptor> keyFields;

		/**
		 * ExpressionKeys that is defined by the full data type.
		 */
		public ExpressionKeys(TypeInformation<T> type) {
			this(SELECT_ALL_CHAR, type);
		}

		/**
		 * Create int-based (non-nested) field position keys on a tuple type.
		 */
		public ExpressionKeys(int keyPosition, TypeInformation<T> type) {
			this(new int[]{keyPosition}, type, false);
		}

		/**
		 * Create int-based (non-nested) field position keys on a tuple type.
		 */
		public ExpressionKeys(int[] keyPositions, TypeInformation<T> type) {
			this(keyPositions, type, false);
		}

		/**
		 * Create int-based (non-nested) field position keys on a tuple type.
		 */
		public ExpressionKeys(int[] keyPositions, TypeInformation<T> type, boolean allowEmpty) {

			if (!type.isTupleType() || !(type instanceof CompositeType)) {
				throw new InvalidProgramException("Specifying keys via field positions is only valid " +
						"for tuple data types. Type: " + type);
			}
			if (type.getArity() == 0) {
				throw new InvalidProgramException("Tuple size must be greater than 0. Size: " + type.getArity());
			}
			if (!allowEmpty && (keyPositions == null || keyPositions.length == 0)) {
				throw new IllegalArgumentException("The grouping fields must not be empty.");
			}

			this.keyFields = new ArrayList<>();

			if (keyPositions == null || keyPositions.length == 0) {
				// use all tuple fields as key fields
				keyPositions = createIncrIntArray(type.getArity());
			} else {
				rangeCheckFields(keyPositions, type.getArity() - 1);
			}
			Preconditions.checkArgument(keyPositions.length > 0, "Grouping fields can not be empty at this point");

			// extract key field types
			CompositeType<T> cType = (CompositeType<T>)type;
			this.keyFields = new ArrayList<>(type.getTotalFields());

			// for each key position, find all (nested) field types
			String[] fieldNames = cType.getFieldNames();
			ArrayList<FlatFieldDescriptor> tmpList = new ArrayList<>();
			for (int keyPos : keyPositions) {
				tmpList.clear();
				// get all flat fields
				cType.getFlatFields(fieldNames[keyPos], 0, tmpList);
				// check if fields are of key type
				for(FlatFieldDescriptor ffd : tmpList) {
					if(!ffd.getType().isKeyType()) {
						throw new InvalidProgramException("This type (" + ffd.getType() + ") cannot be used as key.");
					}
				}
				this.keyFields.addAll(tmpList);
			}
		}

		/**
		 * Create String-based (nested) field expression keys on a composite type.
		 */
		public ExpressionKeys(String keyExpression, TypeInformation<T> type) {
			this(new String[]{keyExpression}, type);
		}

		/**
		 * Create String-based (nested) field expression keys on a composite type.
		 */
		public ExpressionKeys(String[] keyExpressions, TypeInformation<T> type) {
			Preconditions.checkNotNull(keyExpressions, "Field expression cannot be null.");

			this.keyFields = new ArrayList<>(keyExpressions.length);

			if (type instanceof CompositeType){
				CompositeType<T> cType = (CompositeType<T>) type;

				// extract the keys on their flat position
				for (String keyExpr : keyExpressions) {
					if (keyExpr == null) {
						throw new InvalidProgramException("Expression key may not be null.");
					}
					// strip off whitespace
					keyExpr = keyExpr.trim();

					List<FlatFieldDescriptor> flatFields = cType.getFlatFields(keyExpr);

					if (flatFields.size() == 0) {
						throw new InvalidProgramException("Unable to extract key from expression '" + keyExpr + "' on key " + cType);
					}
					// check if all nested fields can be used as keys
					for (FlatFieldDescriptor field : flatFields) {
						if (!field.getType().isKeyType()) {
							throw new InvalidProgramException("This type (" + field.getType() + ") cannot be used as key.");
						}
					}
					// add flat fields to key fields
					keyFields.addAll(flatFields);
				}
			}
			else {
				if (!type.isKeyType()) {
					throw new InvalidProgramException("This type (" + type + ") cannot be used as key.");
				}

				// check that all key expressions are valid
				for (String keyExpr : keyExpressions) {
					if (keyExpr == null) {
						throw new InvalidProgramException("Expression key may not be null.");
					}
					// strip off whitespace
					keyExpr = keyExpr.trim();
					// check that full type is addressed
					if (!(SELECT_ALL_CHAR.equals(keyExpr) || SELECT_ALL_CHAR_SCALA.equals(keyExpr))) {
						throw new InvalidProgramException(
							"Field expression must be equal to '" + SELECT_ALL_CHAR + "' or '" + SELECT_ALL_CHAR_SCALA + "' for non-composite types.");
					}
					// add full type as key
					keyFields.add(new FlatFieldDescriptor(0, type));
				}
			}
		}
		
		@Override
		public int getNumberOfKeyFields() {
			if(keyFields == null) {
				return 0;
			}
			return keyFields.size();
		}

		@Override
		public int[] computeLogicalKeyPositions() {
			int[] logicalKeys = new int[keyFields.size()];
			for (int i = 0; i < keyFields.size(); i++) {
				logicalKeys[i] = keyFields.get(i).getPosition();
			}
			return logicalKeys;
		}

		@Override
		protected TypeInformation<?>[] getKeyFieldTypes() {
			TypeInformation<?>[] fieldTypes = new TypeInformation[keyFields.size()];
			for (int i = 0; i < keyFields.size(); i++) {
				fieldTypes[i] = keyFields.get(i).getType();
			}
			return fieldTypes;
		}

		@Override
		public <E> void validateCustomPartitioner(Partitioner<E> partitioner, TypeInformation<E> typeInfo) {

			if (keyFields.size() != 1) {
				throw new InvalidProgramException("Custom partitioners can only be used with keys that have one key field.");
			}

			if (typeInfo == null) {
				// try to extract key type from partitioner
				try {
					typeInfo = TypeExtractor.getPartitionerTypes(partitioner);
				}
				catch (Throwable t) {
					// best effort check, so we ignore exceptions
				}
			}

			if (typeInfo != null && !(typeInfo instanceof GenericTypeInfo)) {
				// only check type compatibility if type is known and not a generic type

				TypeInformation<?> keyType = keyFields.get(0).getType();
				if (!keyType.equals(typeInfo)) {
					throw new InvalidProgramException("The partitioner is incompatible with the key type. "
										+ "Partitioner type: " + typeInfo + " , key type: " + keyType);
				}
			}
		}

		@Override
		public String toString() {
			Joiner join = Joiner.on('.');
			return "ExpressionKeys: " + join.join(keyFields);
		}

		public static boolean isSortKey(int fieldPos, TypeInformation<?> type) {

			if (!type.isTupleType() || !(type instanceof CompositeType)) {
				throw new InvalidProgramException("Specifying keys via field positions is only valid " +
					"for tuple data types. Type: " + type);
			}
			if (type.getArity() == 0) {
				throw new InvalidProgramException("Tuple size must be greater than 0. Size: " + type.getArity());
			}

			if(fieldPos < 0 || fieldPos >= type.getArity()) {
				throw new IndexOutOfBoundsException("Tuple position is out of range: " + fieldPos);
			}

			TypeInformation<?> sortKeyType = ((CompositeType<?>)type).getTypeAt(fieldPos);
			return sortKeyType.isSortKeyType();
		}

		public static boolean isSortKey(String fieldExpr, TypeInformation<?> type) {

			TypeInformation<?> sortKeyType;

			fieldExpr = fieldExpr.trim();
			if (SELECT_ALL_CHAR.equals(fieldExpr) || SELECT_ALL_CHAR_SCALA.equals(fieldExpr)) {
				sortKeyType = type;
			}
			else {
				if (type instanceof CompositeType) {
					sortKeyType = ((CompositeType<?>) type).getTypeAt(fieldExpr);
				}
				else {
					throw new InvalidProgramException(
						"Field expression must be equal to '" + SELECT_ALL_CHAR + "' or '" + SELECT_ALL_CHAR_SCALA + "' for atomic types.");
				}
			}

			return sortKeyType.isSortKeyType();
		}

	}

	// --------------------------------------------------------------------------------------------



	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------


	private static int[] createIncrIntArray(int numKeys) {
		int[] keyFields = new int[numKeys];
		for (int i = 0; i < numKeys; i++) {
			keyFields[i] = i;
		}
		return keyFields;
	}

	private static void rangeCheckFields(int[] fields, int maxAllowedField) {

		for (int f : fields) {
			if (f < 0 || f > maxAllowedField) {
				throw new IndexOutOfBoundsException("Tuple position is out of range: " + f);
			}
		}
	}

	public static class IncompatibleKeysException extends Exception {
		private static final long serialVersionUID = 1L;
		public static final String SIZE_MISMATCH_MESSAGE = "The number of specified keys is different.";
		
		public IncompatibleKeysException(String message) {
			super(message);
		}

		public IncompatibleKeysException(TypeInformation<?> typeInformation, TypeInformation<?> typeInformation2) {
			super(typeInformation+" and "+typeInformation2+" are not compatible");
		}
	}
}
