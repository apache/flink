/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Static factories for the {@link FieldAccessor} utilities.
 */
@Internal
public class FieldAccessorFactory implements Serializable {

	/**
	 * Creates a {@link FieldAccessor} for the given field position, which can be used to get and set
	 * the specified field on instances of this type.
	 *
	 * @param pos The field position (zero-based)
	 * @param config Configuration object
	 * @param <F> The type of the field to access
	 * @return The created FieldAccessor
	 */
	@Internal
	public static <T, F> FieldAccessor<T, F> getAccessor(TypeInformation<T> typeInfo, int pos, ExecutionConfig config){

		// In case of arrays
		if (typeInfo instanceof BasicArrayTypeInfo || typeInfo instanceof PrimitiveArrayTypeInfo) {
			return new FieldAccessor.ArrayFieldAccessor<>(pos, typeInfo);

		// In case of basic types
		} else if (typeInfo instanceof BasicTypeInfo) {
			if (pos != 0) {
				throw new CompositeType.InvalidFieldReferenceException("The " + ((Integer) pos).toString() + ". field selected on a " +
					"basic type (" + typeInfo.toString() + "). A field expression on a basic type can only select " +
					"the 0th field (which means selecting the entire basic type).");
			}
			@SuppressWarnings("unchecked")
			FieldAccessor<T, F> result = (FieldAccessor<T, F>) new FieldAccessor.SimpleFieldAccessor<>(typeInfo);
			return result;

		// In case of case classes
		} else if (typeInfo.isTupleType() && ((TupleTypeInfoBase) typeInfo).isCaseClass()) {
			TupleTypeInfoBase tupleTypeInfo = (TupleTypeInfoBase) typeInfo;
			@SuppressWarnings("unchecked")
			TypeInformation<F> fieldTypeInfo = (TypeInformation<F>) tupleTypeInfo.getTypeAt(pos);
			return new FieldAccessor.RecursiveProductFieldAccessor<>(
				pos, typeInfo, new FieldAccessor.SimpleFieldAccessor<>(fieldTypeInfo), config);

		// In case of tuples
		} else if (typeInfo.isTupleType()) {
			@SuppressWarnings("unchecked")
			FieldAccessor<T, F> result = new FieldAccessor.SimpleTupleFieldAccessor(pos, typeInfo);
			return result;

		// Default case, PojoType is directed to this statement
		} else {
			throw new CompositeType.InvalidFieldReferenceException("Cannot reference field by position on " + typeInfo.toString()
				+ "Referencing a field by position is supported on tuples, case classes, and arrays. "
				+ "Additionally, you can select the 0th field of a primitive/basic type (e.g. int).");
		}
	}

	/**
	 * Creates a {@link FieldAccessor} for the field that is given by a field expression,
	 * which can be used to get and set the specified field on instances of this type.
	 *
	 * @param field The field expression
	 * @param config Configuration object
	 * @param <F> The type of the field to access
	 * @return The created FieldAccessor
	 */
	@Internal
	public static <T, F> FieldAccessor<T, F> getAccessor(TypeInformation<T> typeInfo, String field, ExecutionConfig config) {

		// In case of arrays
		if (typeInfo instanceof BasicArrayTypeInfo || typeInfo instanceof PrimitiveArrayTypeInfo) {
			try {
				return new FieldAccessor.ArrayFieldAccessor<>(Integer.parseInt(field), typeInfo);
			} catch (NumberFormatException ex) {
				throw new CompositeType.InvalidFieldReferenceException
					("A field expression on an array must be an integer index (that might be given as a string).");
			}

		// In case of basic types
		} else if (typeInfo instanceof BasicTypeInfo) {
			try {
				int pos = field.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR) ? 0 : Integer.parseInt(field);
				return FieldAccessorFactory.getAccessor(typeInfo, pos, config);
			} catch (NumberFormatException ex) {
				throw new CompositeType.InvalidFieldReferenceException("You tried to select the field \"" + field +
					"\" on a " + typeInfo.toString() + ". A field expression on a basic type can only be \"*\" or \"0\"" +
					" (both of which mean selecting the entire basic type).");
			}

		// In case of Pojos
		} else if (typeInfo instanceof PojoTypeInfo) {
			FieldExpression decomp = decomposeFieldExpression(field);
			PojoTypeInfo<?> pojoTypeInfo = (PojoTypeInfo) typeInfo;

			int fieldIndex = pojoTypeInfo.getFieldIndex(decomp.head);

			if (fieldIndex == -1) {
				throw new CompositeType.InvalidFieldReferenceException(
					"Unable to find field \"" + decomp.head + "\" in type " + typeInfo + ".");
			} else {
				PojoField pojoField = pojoTypeInfo.getPojoFieldAt(fieldIndex);
				TypeInformation<?> fieldType = pojoTypeInfo.getTypeAt(fieldIndex);
				if (decomp.tail == null) {
					@SuppressWarnings("unchecked")
					FieldAccessor<F, F> innerAccessor = new FieldAccessor.SimpleFieldAccessor<>((TypeInformation<F>) fieldType);
					return new FieldAccessor.PojoFieldAccessor<>(pojoField.getField(), innerAccessor);
				} else {
					@SuppressWarnings("unchecked")
					FieldAccessor<Object, F> innerAccessor = FieldAccessorFactory
						.getAccessor((TypeInformation<Object>) fieldType, decomp.tail, config);
					return new FieldAccessor.PojoFieldAccessor<>(pojoField.getField(), innerAccessor);
				}
			}
		// In case of case classes
		} else if (typeInfo.isTupleType() && ((TupleTypeInfoBase) typeInfo).isCaseClass()) {
			TupleTypeInfoBase tupleTypeInfo = (TupleTypeInfoBase) typeInfo;
			FieldExpression decomp = decomposeFieldExpression(field);
			int fieldPos = tupleTypeInfo.getFieldIndex(decomp.head);
			if (fieldPos < 0) {
				throw new CompositeType.InvalidFieldReferenceException("Invalid field selected: " + field);
			}

			if (decomp.tail == null){
				return new FieldAccessor.SimpleProductFieldAccessor<>(fieldPos, typeInfo, config);
			} else {
				@SuppressWarnings("unchecked")
				FieldAccessor<Object, F> innerAccessor = getAccessor(tupleTypeInfo.getTypeAt(fieldPos), decomp.tail, config);
				return new FieldAccessor.RecursiveProductFieldAccessor<>(fieldPos, typeInfo, innerAccessor, config);
			}

		// In case of tuples
		} else if (typeInfo.isTupleType() && typeInfo instanceof TupleTypeInfo) {
			TupleTypeInfo tupleTypeInfo = (TupleTypeInfo) typeInfo;
			FieldExpression decomp = decomposeFieldExpression(field);
			int fieldPos = tupleTypeInfo.getFieldIndex(decomp.head);
			if (fieldPos == -1) {
				try {
					fieldPos = Integer.parseInt(decomp.head);
				} catch (NumberFormatException ex) {
					throw new CompositeType.InvalidFieldReferenceException("Tried to select field \"" + decomp.head
						+ "\" on " + typeInfo.toString() + " . Only integer values are allowed here.");
				}
			}
			if (decomp.tail == null) {
				@SuppressWarnings("unchecked")
				FieldAccessor<T, F> result = new FieldAccessor.SimpleTupleFieldAccessor(fieldPos, tupleTypeInfo);
				return result;
			} else {
				@SuppressWarnings("unchecked")
				FieldAccessor<?, F> innerAccessor = getAccessor(tupleTypeInfo.getTypeAt(fieldPos), decomp.tail, config);
				@SuppressWarnings("unchecked")
				FieldAccessor<T, F> result = new FieldAccessor.RecursiveTupleFieldAccessor(fieldPos, innerAccessor, tupleTypeInfo);
				return result;
			}

		// Default statement
		} else {
			throw new CompositeType.InvalidFieldReferenceException("Cannot reference field by field expression on " + typeInfo.toString()
				+ "Field expressions are only supported on POJO types, tuples, and case classes. "
				+ "(See the Flink documentation on what is considered a POJO.)");
		}
	}

	// --------------------------------------------------------------------------------------------------

	private  static final String REGEX_FIELD = "[\\p{L}\\p{Digit}_\\$]*"; // This can start with a digit (because of Tuples)
	private static final String REGEX_NESTED_FIELDS = "(" + REGEX_FIELD + ")(\\.(.+))?";
	private static final String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS
		+ "|\\" + Keys.ExpressionKeys.SELECT_ALL_CHAR
		+ "|\\" + Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	private static FieldExpression decomposeFieldExpression(String fieldExpression) {
		Matcher matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression);
		if (!matcher.matches()) {
			throw new CompositeType.InvalidFieldReferenceException("Invalid field expression \"" + fieldExpression + "\".");
		}

		String head = matcher.group(0);
		if (head.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR) || head.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
			throw new CompositeType.InvalidFieldReferenceException("No wildcards are allowed here.");
		} else {
			head = matcher.group(1);
		}

		String tail = matcher.group(3);

		return new FieldExpression(head, tail);
	}

	/**
	 * Represents a decomposition of a field expression into its first part, and the rest.
	 * E.g. "foo.f1.bar" is decomposed into "foo" and "f1.bar".
	 */
	private static class FieldExpression implements Serializable {

		private static final long serialVersionUID = 1L;

		public String head, tail; // tail can be null, if the field expression had just one part

		FieldExpression(String head, String tail) {
			this.head = head;
			this.tail = tail;
		}
	}
}
