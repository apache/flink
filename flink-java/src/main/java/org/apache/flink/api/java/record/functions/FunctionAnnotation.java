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


package org.apache.flink.api.java.record.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;


/**
 * 
 * <b>NOTE: The Record API is marked as deprecated. It is not being developed anymore and will be removed from
 * the code at some point.
 * See <a href="https://issues.apache.org/jira/browse/FLINK-1106">FLINK-1106</a> for more details.</b>
 * 
 * 
 * 
 * This class defines the semantic assertions that can be added to functions.
 * The assertions are realized as java annotations, to be added to the class declaration of
 * the class that realized the user function. For example, to declare the <i>ConstantFieldsExcept</i> 
 * annotation for a map-type function that realizes a simple absolute function,
 * use it the following way:
 * 
 * <pre>{@code
 * {@literal @}ConstantFieldsExcept(fields={2})
 * public class MyMapper extends MapFunction
 * {
 *     public void map(Record record, Collector out)
 *     {
 *        int value = record.getField(2, IntValue.class).getValue();
		record.setField(2, new IntValue(Math.abs(value)));
		
		out.collect(record);
 *     }
 * }
 * }</pre>
 * 
 * Be aware that some annotations should only be used for functions with as single input 
 * ({@link MapFunction}, {@link ReduceFunction}) and some only for stubs with two inputs 
 * ({@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction}).
 */
@Deprecated
public class FunctionAnnotation {
	
	/**
	 * Specifies the fields of an input record that are unchanged in the output of 
	 * a stub with a single input ( {@link MapFunction}, {@link ReduceFunction}).
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying constant fields.
	 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 * 
	 * This annotation is mutually exclusive with the {@link ConstantFieldsExcept} annotation.
	 * 
	 * If this annotation and the {@link ConstantFieldsExcept} annotation is not set, it is 
	 * assumed that <i>no</i> field is constant.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFields {
		int[] value();
	}
	
	/**
	 * Specifies that all fields of an input record that are unchanged in the output of 
	 * a {@link MapFunction}, or {@link ReduceFunction}).
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying constant fields.
	 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 * 
	 * This annotation is mutually exclusive with the {@link ConstantFieldsExcept} annotation.
	 * 
	 * If this annotation and the {@link ConstantFieldsExcept} annotation is not set, it is 
	 * assumed that <i>no</i> field is constant.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface AllFieldsConstants {}
	
	/**
	 * Specifies the fields of an input record of the first input that are unchanged in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying constant fields.
	 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 *
	 * This annotation is mutually exclusive with the {@link ConstantFieldsFirstExcept} annotation.
	 * 
	 * If this annotation and the {@link ConstantFieldsFirstExcept} annotation is not set, it is 
	 * assumed that <i>no</i> field is constant.
	 * 
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsFirst {
		int[] value();
	}
	
	/**
	 * Specifies the fields of an input record of the second input that are unchanged in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying constant fields.
	 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 *
	 * This annotation is mutually exclusive with the {@link ConstantFieldsSecondExcept} annotation.
	 * 
	 * If this annotation and the {@link ConstantFieldsSecondExcept} annotation is not set, it is 
	 * assumed that <i>no</i> field is constant.
	 * 
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsSecond {
		int[] value();
	}
	
	/**
	 * Specifies the fields of an input record that are changed in the output of 
	 * a stub with a single input ( {@link MapFunction}, {@link ReduceFunction}). All other 
	 * fields are assumed to be constant.
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying constant fields.
	 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 * 
	 * This annotation is mutually exclusive with the {@link ConstantFields} annotation.
	 * 
	 * If this annotation and the {@link ConstantFields} annotation is not set, it is 
	 * assumed that <i>no</i> field is constant.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsExcept {
		int[] value();
	}
	
	/**
	 * Specifies the fields of an input record of the first input that are changed in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * All other fields are assumed to be constant.
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying constant fields.
	 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 *
	 * This annotation is mutually exclusive with the {@link ConstantFieldsFirst} annotation.
	 * 
	 * If this annotation and the {@link ConstantFieldsFirst} annotation is not set, it is 
	 * assumed that <i>no</i> field is constant.
	 * 
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsFirstExcept {
		int[] value();
	}
	
	
	/**
	 * Specifies the fields of an input record of the second input that are changed in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * All other fields are assumed to be constant.
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying constant fields.
	 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 *
	 * This annotation is mutually exclusive with the {@link ConstantFieldsSecond} annotation.
	 * 
	 * If this annotation and the {@link ConstantFieldsSecond} annotation is not set, it is 
	 * assumed that <i>no</i> field is constant.
	 * 
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsSecondExcept {
		int[] value();
	}

	/**
	 * Private constructor to prevent instantiation. This class is intended only as a container.
	 */
	private FunctionAnnotation() {}
	
	// --------------------------------------------------------------------------------------------
	//                                   Function Annotation Handling
	// --------------------------------------------------------------------------------------------
	
	public static SingleInputSemanticProperties readSingleConstantAnnotations(UserCodeWrapper<?> udf) {
		
		// get constantSet annotation from stub
		AllFieldsConstants allConstants = udf.getUserCodeAnnotation(AllFieldsConstants.class);
		ConstantFields constantSet = udf.getUserCodeAnnotation(ConstantFields.class);
		ConstantFieldsExcept notConstantSet = udf.getUserCodeAnnotation(ConstantFieldsExcept.class);

		if (notConstantSet != null && (constantSet != null || allConstants != null)) {
			throw new RuntimeException("Either ConstantFields or ConstantFieldsExcept can be specified, not both.");
		}
		
		// extract notConstantSet from annotation
		if (notConstantSet != null) {
			FieldSet nonConstant = new FieldSet(notConstantSet.value());
			return new ImplicitlyForwardingSingleInputSemanticProperties(nonConstant);
		}
		
		// extract notConstantSet from annotation
		if (allConstants != null) {
			return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
		}
		
		SingleInputSemanticProperties semanticProperties = new SingleInputSemanticProperties();
		
		// extract constantSet from annotation
		if (constantSet != null) {
			for (int value: constantSet.value()) {
				semanticProperties.addForwardedField(value,value);
			}
		}
		
		return semanticProperties;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static DualInputSemanticProperties readDualConstantAnnotations(UserCodeWrapper<?> udf) {
		ImplicitlyForwardingTwoInputSemanticProperties semanticProperties = new ImplicitlyForwardingTwoInputSemanticProperties();

		// get readSet annotation from stub
		ConstantFieldsFirst constantSet1Annotation = udf.getUserCodeAnnotation(ConstantFieldsFirst.class);
		ConstantFieldsSecond constantSet2Annotation = udf.getUserCodeAnnotation(ConstantFieldsSecond.class);
		
		// get readSet annotation from stub
		ConstantFieldsFirstExcept notConstantSet1Annotation = udf.getUserCodeAnnotation(ConstantFieldsFirstExcept.class);
		ConstantFieldsSecondExcept notConstantSet2Annotation = udf.getUserCodeAnnotation(ConstantFieldsSecondExcept.class);
		
		
		if (notConstantSet1Annotation != null && constantSet1Annotation != null) {
			throw new RuntimeException("Either ConstantFieldsFirst or ConstantFieldsFirstExcept can be specified, not both.");
		}
		
		if (constantSet2Annotation != null && notConstantSet2Annotation != null) {
			throw new RuntimeException("Either ConstantFieldsSecond or ConstantFieldsSecondExcept can be specified, not both.");
		}
		
		
		// extract readSets from annotations
		if(notConstantSet1Annotation != null) {
			semanticProperties.setImplicitlyForwardingFirstExcept(new FieldSet(notConstantSet1Annotation.value()));
		}
		
		if(notConstantSet2Annotation != null) {
			semanticProperties.setImplicitlyForwardingSecondExcept(new FieldSet(notConstantSet2Annotation.value()));
		}
		
		// extract readSets from annotations
		if (constantSet1Annotation != null) {
			for(int value: constantSet1Annotation.value()) {
				semanticProperties.addForwardedField(0, value, value);
			}
		}
		
		if (constantSet2Annotation != null) {
			for(int value: constantSet2Annotation.value()) {
				semanticProperties.addForwardedField(1, value, value);
			}
		}
		
		return semanticProperties;
	}


	private static final class ImplicitlyForwardingSingleInputSemanticProperties extends SingleInputSemanticProperties {

		private static final long serialVersionUID = 1l;

		private FieldSet nonForwardedFields;

		private ImplicitlyForwardingSingleInputSemanticProperties(FieldSet nonForwardedFields) {
			this.nonForwardedFields = nonForwardedFields;
		}

		@Override
		public FieldSet getForwardingTargetFields(int input, int sourceField) {

			if (input != 0) {
				throw new IndexOutOfBoundsException();
			}

			if (nonForwardedFields == null) {
				return super.getForwardingTargetFields(input, sourceField);
			} else {
				if (this.nonForwardedFields.contains(sourceField)) {
					return FieldSet.EMPTY_SET;
				} else {
					return new FieldSet(sourceField);
				}
			}
		}

		@Override
		public int getForwardingSourceField(int input, int targetField) {

			if (input != 0) {
				throw new IndexOutOfBoundsException();
			}

			if (nonForwardedFields == null) {
				return super.getForwardingSourceField(input, targetField);
			} else {
				if (this.nonForwardedFields.contains(targetField)) {
					return -1;
				} else {
					return targetField;
				}
			}
		}

		@Override
		public FieldSet getReadFields(int input) {
			return null;
		}

		@Override
		public void addForwardedField(int sourceField, int destinationField) {
			if (this.nonForwardedFields == null) {
				super.addForwardedField(sourceField, destinationField);
			} else {
				throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
						"(such as through the ConstantFieldsFirstExcept annotation), you cannot manually add forwarded fields.");
			}
		}

	}

	private static final class ImplicitlyForwardingTwoInputSemanticProperties extends DualInputSemanticProperties {
		private static final long serialVersionUID = 1L;

		private FieldSet nonForwardedFields1;
		private FieldSet nonForwardedFields2;

		private ImplicitlyForwardingTwoInputSemanticProperties() {}

		public void setImplicitlyForwardingFirstExcept(FieldSet nonForwardedFields) {
			this.nonForwardedFields1 = nonForwardedFields;
		}

		public void setImplicitlyForwardingSecondExcept(FieldSet nonForwardedFields) {
			this.nonForwardedFields2 = nonForwardedFields;
		}

		@Override
		public FieldSet getForwardingTargetFields(int input, int sourceField) {

			if(input != 0 && input != 1) {
				throw new IndexOutOfBoundsException();
			} else if (input == 0) {

				if (this.nonForwardedFields1 == null) {
					return super.getForwardingTargetFields(0, sourceField);
				}
				else {
					if (this.nonForwardedFields1.contains(sourceField)) {
						return FieldSet.EMPTY_SET;
					} else {
						return new FieldSet(sourceField);
					}
				}
			} else {

				if (this.nonForwardedFields2 == null) {
					return super.getForwardingTargetFields(1, sourceField);
				}
				else {
					if (this.nonForwardedFields2.contains(sourceField)) {
						return FieldSet.EMPTY_SET;
					} else {
						return new FieldSet(sourceField);
					}
				}
			}
		}

		@Override
		public void addForwardedField(int input, int sourceField, int destinationField) {
			if (input != 0 && input != 1) {
				throw new IndexOutOfBoundsException();
			} else if (input == 0) {
				if (this.nonForwardedFields1 == null) {
					super.addForwardedField(0, sourceField, destinationField);
				} else {
					throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
							"(such as through the ConstantFieldsFirstExcept annotation), you cannot manually add forwarded fields.");
				}
			} else {
				if (this.nonForwardedFields2 == null) {
					super.addForwardedField(1, sourceField, destinationField);
				} else {
					throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
							"(such as through the ConstantFieldsFirstExcept annotation), you cannot manually add forwarded fields.");
				}
			}
		}

		@Override
		public FieldSet getReadFields(int input) {
			return null;
		}

	}
}
