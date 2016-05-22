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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.InvalidFieldReferenceException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.runtime.FieldSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * These classes encapsulate the logic of accessing a field specified by the user as either an index
 * or a field expression string. TypeInformation can also be requested for the field.
 * The position index might specify a field of a Tuple, an array, or a simple type (only "0th field").
 *
 * Field expressions that specify nested fields (e.g. "f1.a.foo") result in nested field accessors.
 * These penetrate one layer, and then delegate the rest of the work to an "innerAccesor".
 * (see PojoFieldAccessor, RecursiveTupleFieldAccessor, ProductFieldAccessor)
 */
@PublicEvolving
public abstract class FieldAccessor<T, F> implements Serializable {

	private static final long serialVersionUID = 1L;

	protected TypeInformation fieldType;

	/**
	 * Gets the TypeInformation for the type of the field.
	 * Note: For an array of a primitive type, it returns the corresponding basic type (Integer for int[]).
	 */
	@SuppressWarnings("unchecked")
	public TypeInformation<F> getFieldType() {
		return fieldType;
	}


	/**
	 * Gets the value of the field (specified in the constructor) of the given record.
	 * @param record The record on which the field will be accessed
	 * @return The value of the field.
	 */
	public abstract F get(T record);

	/**
	 * Sets the field (specified in the constructor) of the given record to the given value.
	 *
	 * Warning: This might modify the original object, or might return a new object instance.
	 * (This is necessary, because the record might be immutable.)
	 *
	 * @param record The record to modify
	 * @param fieldValue The new value of the field
	 * @return A record that has the given field value. (this might be a new instance or the original)
	 */
	public abstract T set(T record, F fieldValue);


	// --------------------------------------------------------------------------------------------------


	/**
	 * This is when the entire record is considered as a single field. (eg. field 0 of a basic type, or a
	 * field of a POJO that is itself some composite type but is not further decomposed)
	 */
	public final static class SimpleFieldAccessor<T> extends FieldAccessor<T, T> {

		private static final long serialVersionUID = 1L;

		public SimpleFieldAccessor(TypeInformation<T> typeInfo) {
			checkNotNull(typeInfo, "typeInfo must not be null.");

			this.fieldType = typeInfo;
		}

		@Override
		public T get(T record) {
			return record;
		}

		@Override
		public T set(T record, T fieldValue) {
			return fieldValue;
		}
	}

	public final static class ArrayFieldAccessor<T, F> extends FieldAccessor<T, F> {

		private static final long serialVersionUID = 1L;

		private final int pos;

		public ArrayFieldAccessor(int pos, TypeInformation typeInfo) {
			if(pos < 0) {
				throw new InvalidFieldReferenceException("The " + ((Integer) pos).toString() + ". field selected on" +
					" an array, which is an invalid index.");
			}
			checkNotNull(typeInfo, "typeInfo must not be null.");

			this.pos = pos;
			this.fieldType = BasicTypeInfo.getInfoFor(typeInfo.getTypeClass().getComponentType());
		}

		@SuppressWarnings("unchecked")
		@Override
		public F get(T record) {
			return (F) Array.get(record, pos);
		}

		@Override
		public T set(T record, F fieldValue) {
			Array.set(record, pos, fieldValue);
			return record;
		}
	}

	/**
	 * There are two versions of TupleFieldAccessor, differing in whether there is an other
	 * FieldAccessor nested inside. The no inner accessor version is probably a little faster.
	 */
	static final class SimpleTupleFieldAccessor<T, F> extends FieldAccessor<T, F> {

		private static final long serialVersionUID = 1L;

		private final int pos;

		SimpleTupleFieldAccessor(int pos, TypeInformation<T> typeInfo) {
			int arity = ((TupleTypeInfo)typeInfo).getArity();
			if(pos < 0 || pos >= arity) {
				throw new InvalidFieldReferenceException(
					"Tried to select " + ((Integer) pos).toString() + ". field on \"" +
					typeInfo.toString() + "\", which is an invalid index.");
			}
			checkNotNull(typeInfo, "typeInfo must not be null.");

			this.pos = pos;
			this.fieldType = ((TupleTypeInfo)typeInfo).getTypeAt(pos);
		}

		@SuppressWarnings("unchecked")
		@Override
		public F get(T record) {
			final Tuple tuple = (Tuple) record;
			return (F) tuple.getField(pos);
		}

		@Override
		public T set(T record, F fieldValue) {
			final Tuple tuple = (Tuple) record;
			tuple.setField(fieldValue, pos);
			return record;
		}
	}

	/**
	 * @param <T> The Tuple type
	 * @param <R> The field type at the first level
	 * @param <F> The field type at the innermost level
	 */
	static final class RecursiveTupleFieldAccessor<T, R, F> extends FieldAccessor<T, F> {

		private static final long serialVersionUID = 1L;

		private final int pos;
		private final FieldAccessor<R, F> innerAccessor;

		RecursiveTupleFieldAccessor(int pos, FieldAccessor<R, F> innerAccessor) {
			if(pos < 0) {
				throw new InvalidFieldReferenceException("Tried to select " + ((Integer) pos).toString() + ". field.");
			}
			checkNotNull(innerAccessor, "innerAccessor must not be null.");

			this.pos = pos;
			this.innerAccessor = innerAccessor;
			this.fieldType = innerAccessor.fieldType;
		}

		@Override
		public F get(T record) {
			final Tuple tuple = (Tuple) record;
			final R inner = tuple.getField(pos);
			return innerAccessor.get(inner);
		}

		@Override
		public T set(T record, F fieldValue) {
			final Tuple tuple = (Tuple) record;
			final R inner = tuple.getField(pos);
			tuple.setField(innerAccessor.set(inner, fieldValue), pos);
			return record;
		}
	}

	/**
	 * @param <T> The POJO type
	 * @param <R> The field type at the first level
	 * @param <F> The field type at the innermost level
	 */
	static final class PojoFieldAccessor<T, R, F> extends FieldAccessor<T, F> {

		private static final long serialVersionUID = 1L;

		private transient Field field;
		private final FieldAccessor<R, F> innerAccessor;

		PojoFieldAccessor(Field field, FieldAccessor<R, F> innerAccessor) {
			checkNotNull(field, "field must not be null.");
			checkNotNull(innerAccessor, "innerAccessor must not be null.");

			this.field = field;
			this.innerAccessor = innerAccessor;
			this.fieldType = innerAccessor.fieldType;
		}

		@Override
		public F get(T pojo) {
			try {
				@SuppressWarnings("unchecked")
				final R inner = (R)field.get(pojo);
				return innerAccessor.get(inner);
			} catch (IllegalAccessException iaex) {
				throw new RuntimeException("This should not happen since we call setAccesssible(true) in readObject."
						+ " fields: " + field + " obj: " + pojo);
			}
		}

		@Override
		public T set(T pojo, F valueToSet) {
			try {
				@SuppressWarnings("unchecked")
				final R inner = (R)field.get(pojo);
				field.set(pojo, innerAccessor.set(inner, valueToSet));
				return pojo;
			} catch (IllegalAccessException iaex) {
				throw new RuntimeException("This should not happen since we call setAccesssible(true) in readObject."
						+ " fields: " + field + " obj: " + pojo);
			}
		}

		private void writeObject(ObjectOutputStream out)
				throws IOException, ClassNotFoundException {
			out.defaultWriteObject();
			FieldSerializer.serializeField(field, out);
		}

		private void readObject(ObjectInputStream in)
				throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			field = FieldSerializer.deserializeField(in);
		}
	}


	// --------------------------------------------------------------------------------------------------

	private final static String REGEX_FIELD = "[\\p{L}\\p{Digit}_\\$]*"; // This can start with a digit (because of Tuples)
	private final static String REGEX_NESTED_FIELDS = "("+REGEX_FIELD+")(\\.(.+))?";
	private final static String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS
		+"|\\"+ Keys.ExpressionKeys.SELECT_ALL_CHAR
		+"|\\"+ Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	public static FieldExpression decomposeFieldExpression(String fieldExpression) {
		Matcher matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression);
		if(!matcher.matches()) {
			throw new InvalidFieldReferenceException("Invalid field expression \""+fieldExpression+"\".");
		}

		String head = matcher.group(0);
		if(head.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR) || head.equals(Keys.ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
			throw new InvalidFieldReferenceException("No wildcards are allowed here.");
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
	public static class FieldExpression implements Serializable {

		private static final long serialVersionUID = 1L;

		public String head, tail; // tail can be null, if the field expression had just one part

		FieldExpression(String head, String tail) {
			this.head = head;
			this.tail = tail;
		}
	}
}
