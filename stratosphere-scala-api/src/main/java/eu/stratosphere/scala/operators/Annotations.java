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

package eu.stratosphere.scala.operators;

import java.lang.annotation.Annotation;
import java.util.Arrays;

import eu.stratosphere.api.record.functions.FunctionAnnotation;
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
	private static class ConstantFields extends Fields<FunctionAnnotation.ConstantFields> implements
			FunctionAnnotation.ConstantFields {

		public ConstantFields(int[] fields) {
			super(FunctionAnnotation.ConstantFields.class, fields);
		}

		@Override
		protected int[] getOtherFields(FunctionAnnotation.ConstantFields other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsFirst extends Fields<FunctionAnnotation.ConstantFieldsFirst> implements
			FunctionAnnotation.ConstantFieldsFirst {

		public ConstantFieldsFirst(int[] fields) {
			super(FunctionAnnotation.ConstantFieldsFirst.class, fields);
		}

		@Override
		protected int[] getOtherFields(FunctionAnnotation.ConstantFieldsFirst other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsSecond extends Fields<FunctionAnnotation.ConstantFieldsSecond> implements
			FunctionAnnotation.ConstantFieldsSecond {

		public ConstantFieldsSecond(int[] fields) {
			super(FunctionAnnotation.ConstantFieldsSecond.class, fields);
		}

		@Override
		protected int[] getOtherFields(FunctionAnnotation.ConstantFieldsSecond other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsExcept extends Fields<FunctionAnnotation.ConstantFieldsExcept> implements
			FunctionAnnotation.ConstantFieldsExcept {

		public ConstantFieldsExcept(int[] fields) {
			super(FunctionAnnotation.ConstantFieldsExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(FunctionAnnotation.ConstantFieldsExcept other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsFirstExcept extends Fields<FunctionAnnotation.ConstantFieldsFirstExcept> implements
			FunctionAnnotation.ConstantFieldsFirstExcept {

		public ConstantFieldsFirstExcept(int[] fields) {
			super(FunctionAnnotation.ConstantFieldsFirstExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(FunctionAnnotation.ConstantFieldsFirstExcept other) {
			return other.value();
		}
	}

	@SuppressWarnings("all")
	private static class ConstantFieldsSecondExcept extends Fields<FunctionAnnotation.ConstantFieldsSecondExcept> implements
			FunctionAnnotation.ConstantFieldsSecondExcept {

		public ConstantFieldsSecondExcept(int[] fields) {
			super(FunctionAnnotation.ConstantFieldsSecondExcept.class, fields);
		}

		@Override
		protected int[] getOtherFields(FunctionAnnotation.ConstantFieldsSecondExcept other) {
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