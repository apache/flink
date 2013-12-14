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

package eu.stratosphere.scala.contracts;

import java.lang.annotation.Annotation;
import java.util.Arrays;

import eu.stratosphere.api.functions.StubAnnotation;
import eu.stratosphere.api.record.operators.ReduceOperator;

public class Annotations {

	public static Annotation getConstantFields(int[] fields) {
		return new ConstantFields(fields);
	}

	public static Annotation getConstantFieldsFirst(int[] fields) {
		return new ConstantFieldsFirst(fields);
	}

	public static Annotation getConstantFieldsSecond(int[] fields) {
		return new ConstantFieldsSecond(fields);
	}

	public static Annotation getConstantFieldsExcept(int[] fields) {
		return new ConstantFieldsExcept(fields);
	}

	public static Annotation getConstantFieldsFirstExcept(int[] fields) {
		return new ConstantFieldsFirstExcept(fields);
	}

	public static Annotation getConstantFieldsSecondExcept(int[] fields) {
		return new ConstantFieldsSecondExcept(fields);
	}

	public static Annotation getCombinable() {
		return new Combinable();
	}

	private static abstract class Fields<T extends Annotation> implements Annotation {

		private final Class<T> clazz;

		private final int[] fields;

		public Fields(Class<T> clazz, int[] fields) {
			this.clazz = clazz;
			this.fields = fields;
		}

		public int[] value() {
			return fields;
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return clazz;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean equals(Object obj) {
			if (obj == null || !annotationType().isAssignableFrom(obj.getClass()))
				return false;

			if (!annotationType().equals(((Annotation) obj).annotationType()))
				return false;

			int[] otherFields = getOtherFields((T) obj);
			return Arrays.equals(fields, otherFields);
		}

		protected abstract int[] getOtherFields(T other);

		@Override
		public int hashCode() {
			return 0xf16cd51b ^ Arrays.hashCode(fields);
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFields extends Fields<StubAnnotation.ConstantFields> implements
			StubAnnotation.ConstantFields {

		public ConstantFields(int[] fields) {
			super(StubAnnotation.ConstantFields.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFields other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsFirst extends Fields<StubAnnotation.ConstantFieldsFirst> implements
			StubAnnotation.ConstantFieldsFirst {

		public ConstantFieldsFirst(int[] fields) {
			super(StubAnnotation.ConstantFieldsFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsFirst other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsSecond extends Fields<StubAnnotation.ConstantFieldsSecond> implements
			StubAnnotation.ConstantFieldsSecond {

		public ConstantFieldsSecond(int[] fields) {
			super(StubAnnotation.ConstantFieldsSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsSecond other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsExcept extends Fields<StubAnnotation.ConstantFieldsExcept> implements
			StubAnnotation.ConstantFieldsExcept {

		public ConstantFieldsExcept(int[] fields) {
			super(StubAnnotation.ConstantFieldsExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsExcept other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsFirstExcept extends Fields<StubAnnotation.ConstantFieldsFirstExcept> implements
			StubAnnotation.ConstantFieldsFirstExcept {

		public ConstantFieldsFirstExcept(int[] fields) {
			super(StubAnnotation.ConstantFieldsFirstExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsFirstExcept other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsSecondExcept extends Fields<StubAnnotation.ConstantFieldsSecondExcept> implements
			StubAnnotation.ConstantFieldsSecondExcept {

		public ConstantFieldsSecondExcept(int[] fields) {
			super(StubAnnotation.ConstantFieldsSecondExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(StubAnnotation.ConstantFieldsSecondExcept other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class Combinable implements Annotation, ReduceOperator.Combinable {

		public Combinable() {
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return ReduceOperator.Combinable.class;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !annotationType().isAssignableFrom(obj.getClass()))
				return false;

			if (!annotationType().equals(((Annotation) obj).annotationType()))
				return false;

			return true;
		}

		@Override
		public int hashCode() {
			return 0;
		}
	}
}