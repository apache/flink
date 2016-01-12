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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Joiner;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.AtomicType;
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
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;


public abstract class Keys<T> {
	private static final Logger LOG = LoggerFactory.getLogger(Keys.class);

	public abstract int getNumberOfKeyFields();

	public boolean isEmpty() {
		return getNumberOfKeyFields() == 0;
	}
	
	/**
	 * Check if two sets of keys are compatible to each other (matching types, key counts)
	 */
	public abstract boolean areCompatible(Keys<?> other) throws IncompatibleKeysException;
	
	public abstract int[] computeLogicalKeyPositions();
	
	public abstract <E> void validateCustomPartitioner(Partitioner<E> partitioner, TypeInformation<E> typeInfo);
	
	
	// --------------------------------------------------------------------------------------------
	//  Specializations for expression-based / extractor-based grouping
	// --------------------------------------------------------------------------------------------
	
	
	public static class SelectorFunctionKeys<T, K> extends Keys<T> {

		private final KeySelector<T, K> keyExtractor;
		private final TypeInformation<T> inputType;
		private final TypeInformation<K> keyType;
		private final int[] logicalKeyFields;

		public SelectorFunctionKeys(KeySelector<T, K> keyExtractor, TypeInformation<T> inputType, TypeInformation<K> keyType) {
			if (keyExtractor == null) {
				throw new NullPointerException("Key extractor must not be null.");
			}
			if (keyType == null) {
				throw new NullPointerException("Key type must not be null.");
			}

			this.keyExtractor = keyExtractor;
			this.inputType = inputType;
			this.keyType = keyType;

			if(!keyType.isKeyType()) {
				throw new InvalidProgramException("Return type "+keyType+" of KeySelector "+keyExtractor.getClass()+" is not a valid key type");
			}

			// we have to handle a special case here:
			// if the keyType is a composite type, we need to select the full type with all its fields.
			if(keyType instanceof CompositeType) {
				ExpressionKeys<K> ek = new ExpressionKeys<>(new String[]{ExpressionKeys.SELECT_ALL_CHAR}, keyType);
				logicalKeyFields = ek.computeLogicalKeyPositions();
			} else {
				logicalKeyFields = new int[] {0};
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
			return logicalKeyFields.length;
		}

		@Override
		public boolean areCompatible(Keys<?> other) throws IncompatibleKeysException {
			
			if (other instanceof SelectorFunctionKeys) {
				@SuppressWarnings("unchecked")
				SelectorFunctionKeys<?, K> sfk = (SelectorFunctionKeys<?, K>) other;

				return sfk.keyType.equals(this.keyType);
			}
			else if (other instanceof ExpressionKeys) {
				ExpressionKeys<?> expressionKeys = (ExpressionKeys<?>) other;
				
				if(keyType.isTupleType()) {
					// special case again:
					TupleTypeInfoBase<?> tupleKeyType = (TupleTypeInfoBase<?>) keyType;
					List<FlatFieldDescriptor> keyTypeFields = tupleKeyType.getFlatFields(ExpressionKeys.SELECT_ALL_CHAR);
					if(expressionKeys.keyFields.size() != keyTypeFields.size()) {
						throw new IncompatibleKeysException(IncompatibleKeysException.SIZE_MISMATCH_MESSAGE);
					}
					for(int i=0; i < expressionKeys.keyFields.size(); i++) {
						if(!expressionKeys.keyFields.get(i).getType().equals(keyTypeFields.get(i).getType())) {
							throw new IncompatibleKeysException(expressionKeys.keyFields.get(i).getType(), keyTypeFields.get(i).getType() );
						}
					}
					return true;
				}
				if(expressionKeys.getNumberOfKeyFields() != 1) {
					throw new IncompatibleKeysException("Key selector functions are only compatible to one key");
				}
				
				if(expressionKeys.keyFields.get(0).getType().equals(this.keyType)) {
					return true;
				} else {
					throw new IncompatibleKeysException(expressionKeys.keyFields.get(0).getType(), this.keyType);
				}
			} else {
				throw new IncompatibleKeysException("The key is not compatible with "+other);
			}
		}

		@Override
		public int[] computeLogicalKeyPositions() {
			return logicalKeyFields;
		}
		
		@Override
		public <E> void validateCustomPartitioner(Partitioner<E> partitioner, TypeInformation<E> typeInfo) {
			if (logicalKeyFields.length != 1) {
				throw new InvalidProgramException("Custom partitioners can only be used with keys that have one key field.");
			}
			
			if (typeInfo == null) {
				try {
					typeInfo = TypeExtractor.getPartitionerTypes(partitioner);
				}
				catch (Throwable t) {
					// best effort check, so we ignore exceptions
				}
			}
			
			if (typeInfo != null && !(typeInfo instanceof GenericTypeInfo) && (!keyType.equals(typeInfo))) {
				throw new InvalidProgramException("The partitioner is incompatible with the key type. "
						+ "Partitioner type: " + typeInfo + " , key type: " + keyType);
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
	 * Represents (nested) field access through string and integer-based keys for Composite Types (Tuple or Pojo)
	 */
	public static class ExpressionKeys<T> extends Keys<T> {
		
		public static final String SELECT_ALL_CHAR = "*";
		public static final String SELECT_ALL_CHAR_SCALA = "_";
		
		/**
		 * Flattened fields representing keys fields
		 */
		private List<FlatFieldDescriptor> keyFields;
		
		/**
		 * two constructors for field-based (tuple-type) keys
		 */
		public ExpressionKeys(int[] groupingFields, TypeInformation<T> type) {
			this(groupingFields, type, false);
		}

		// int-defined field
		public ExpressionKeys(int[] groupingFields, TypeInformation<T> type, boolean allowEmpty) {
			if (!type.isTupleType()) {
				throw new InvalidProgramException("Specifying keys via field positions is only valid " +
						"for tuple data types. Type: " + type);
			}
			if (type.getArity() == 0) {
				throw new InvalidProgramException("Tuple size must be greater than 0. Size: " + type.getArity());
			}

			if (!allowEmpty && (groupingFields == null || groupingFields.length == 0)) {
				throw new IllegalArgumentException("The grouping fields must not be empty.");
			}
			// select all fields. Therefore, set all fields on this tuple level and let the logic handle the rest
			// (makes type assignment easier).
			if (groupingFields == null || groupingFields.length == 0) {
				groupingFields = new int[type.getArity()];
				for (int i = 0; i < groupingFields.length; i++) {
					groupingFields[i] = i;
				}
			} else {
				groupingFields = rangeCheckFields(groupingFields, type.getArity() -1);
			}
			Preconditions.checkArgument(groupingFields.length > 0, "Grouping fields can not be empty at this point");
			
			keyFields = new ArrayList<>(type.getTotalFields());
			// for each key, find the field:
			for (int keyPos : groupingFields) {
				int offset = 0;
				for (int i = 0; i < type.getArity(); i++) {

					TypeInformation<?> fieldType = ((CompositeType<?>) type).getTypeAt(i);
					if (i < keyPos) {
						// not yet there, increment key offset
						offset += fieldType.getTotalFields();
					} else {
						// arrived at key position
						if (!fieldType.isKeyType()) {
							throw new InvalidProgramException("This type (" + fieldType + ") cannot be used as key.");
						}
						if (fieldType instanceof CompositeType) {
							// add all nested fields of composite type
							((CompositeType<?>) fieldType).getFlatFields("*", offset, keyFields);
						} else if (fieldType instanceof AtomicType) {
							// add atomic type field
							keyFields.add(new FlatFieldDescriptor(offset, fieldType));
						} else {
							// type should either be composite or atomic
							throw new InvalidProgramException("Field type is neither CompositeType nor AtomicType: " + fieldType);
						}
						// go to next key
						break;
					}
				}
			}
			keyFields = removeNullElementsFromList(keyFields);
		}

		public static <R> List<R> removeNullElementsFromList(List<R> in) {
			List<R> elements = new ArrayList<>();
			for(R e: in) {
				if(e != null) {
					elements.add(e);
				}
			}
			return elements;
		}
		
		/**
		 * Create ExpressionKeys from String-expressions
		 */
		public ExpressionKeys(String[] expressionsIn, TypeInformation<T> type) {
			Preconditions.checkNotNull(expressionsIn, "Field expression cannot be null.");

			if (type instanceof AtomicType) {
				if (!type.isKeyType()) {
					throw new InvalidProgramException("This type (" + type + ") cannot be used as key.");
				} else if (expressionsIn.length != 1 || !(Keys.ExpressionKeys.SELECT_ALL_CHAR.equals(expressionsIn[0]) || Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA.equals(expressionsIn[0]))) {
					throw new InvalidProgramException("Field expression for atomic type must be equal to '*' or '_'.");
				}

				keyFields = new ArrayList<>(1);
				keyFields.add(new FlatFieldDescriptor(0, type));
			} else {
				CompositeType<T> cType = (CompositeType<T>) type;

				String[] expressions = removeDuplicates(expressionsIn);
				if(expressionsIn.length != expressions.length) {
					LOG.warn("The key expressions contained duplicates. They are now unique");
				}
				// extract the keys on their flat position
				keyFields = new ArrayList<>(expressions.length);
				for (String expression : expressions) {
					List<FlatFieldDescriptor> keys = cType.getFlatFields(expression); // use separate list to do a size check
					for (FlatFieldDescriptor key : keys) {
						TypeInformation<?> keyType = key.getType();
						if (!keyType.isKeyType()) {
							throw new InvalidProgramException("This type (" + key.getType() + ") cannot be used as key.");
						}
						if (!(keyType instanceof AtomicType || keyType instanceof CompositeType)) {
							throw new InvalidProgramException("Field type is neither CompositeType nor AtomicType: " + keyType);
						}
					}
					if (keys.size() == 0) {
						throw new InvalidProgramException("Unable to extract key from expression '" + expression + "' on key " + cType);
					}
					keyFields.addAll(keys);
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
		public boolean areCompatible(Keys<?> other) throws IncompatibleKeysException {

			if (other instanceof ExpressionKeys) {
				ExpressionKeys<?> oKey = (ExpressionKeys<?>) other;

				if(oKey.getNumberOfKeyFields() != this.getNumberOfKeyFields() ) {
					throw new IncompatibleKeysException(IncompatibleKeysException.SIZE_MISMATCH_MESSAGE);
				}
				for(int i=0; i < this.keyFields.size(); i++) {
					if(!this.keyFields.get(i).getType().equals(oKey.keyFields.get(i).getType())) {
						throw new IncompatibleKeysException(this.keyFields.get(i).getType(), oKey.keyFields.get(i).getType() );
					}
				}
				return true;
			} else if(other instanceof SelectorFunctionKeys<?, ?>) {
				return other.areCompatible(this);
			} else {
				throw new IncompatibleKeysException("The key is not compatible with "+other);
			}
		}

		@Override
		public int[] computeLogicalKeyPositions() {
			List<Integer> logicalKeys = new ArrayList<>();
			for (FlatFieldDescriptor kd : keyFields) {
				logicalKeys.add(kd.getPosition());
			}
			return Ints.toArray(logicalKeys);
		}
		
		@Override
		public <E> void validateCustomPartitioner(Partitioner<E> partitioner, TypeInformation<E> typeInfo) {
			if (keyFields.size() != 1) {
				throw new InvalidProgramException("Custom partitioners can only be used with keys that have one key field.");
			}
			
			if (typeInfo == null) {
				try {
					typeInfo = TypeExtractor.getPartitionerTypes(partitioner);
				}
				catch (Throwable t) {
					// best effort check, so we ignore exceptions
				}
			}
			
			if (typeInfo != null && !(typeInfo instanceof GenericTypeInfo)) {
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
	}
	
	private static String[] removeDuplicates(String[] in) {
		List<String> ret = new LinkedList<>();
		for(String el : in) {
			if(!ret.contains(el)) {
				ret.add(el);
			}
		}
		return ret.toArray(new String[ret.size()]);
	}
	// --------------------------------------------------------------------------------------------
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------


	private static int[] rangeCheckFields(int[] fields, int maxAllowedField) {

		// range check and duplicate eliminate
		int i = 1, k = 0;
		int last = fields[0];

		if (last < 0 || last > maxAllowedField) {
			throw new IllegalArgumentException("Tuple position is out of range: " + last);
		}

		for (; i < fields.length; i++) {
			if (fields[i] < 0 || fields[i] > maxAllowedField) {
				throw new IllegalArgumentException("Tuple position is out of range.");
			}
			if (fields[i] != last) {
				k++;
				last = fields[i];
				fields[k] = fields[i];
			}
		}

		// check if we eliminated something
		if (k == fields.length - 1) {
			return fields;
		} else {
			return Arrays.copyOfRange(fields, 0, k+1);
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
