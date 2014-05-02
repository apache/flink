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

package eu.stratosphere.api.java.record.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;


/**
 * This class defines the semantic assertions that can be added to functions.
 * The assertions are realized as java annotations, to be added to the class declaration of
 * the class that realized the user function. For example, to declare the <i>ConstantFieldsExcept</i> 
 * annotation for a map-type function that realizes a simple absolute function,
 * use it the following way:
 * 
 * <pre><blockquote>
 * \@ConstantFieldsExcept(fields={2})
 * public class MyMapper extends MapFunction
 * {
 *     public void map(Record record, Collector out)
 *     {
 *        int value = record.getField(2, IntValue.class).getValue();
		record.setField(2, new IntValue(Math.abs(value)));
		
		out.collect(record);
 *     }
 * }
 * </blockquote></pre>
 * 
 * Be aware that some annotations should only be used for functions with as single input 
 * ({@link MapFunction}, {@link ReduceFunction}) and some only for stubs with two inputs 
 * ({@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction}).
 */
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
			FieldSet nonConstant = new FieldSet();
			return new ImplicitlyForwardingSingleInputSemanticProperties(nonConstant);
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
				semanticProperties.addForwardedField1(value, value);
			}
		}
		
		if (constantSet2Annotation != null) {
			for(int value: constantSet2Annotation.value()) {
				semanticProperties.addForwardedField2(value, value);
			}
		}
		
		return semanticProperties;
	}
	
	
	private static final class ImplicitlyForwardingSingleInputSemanticProperties extends SingleInputSemanticProperties {
		private static final long serialVersionUID = 1L;
		
		private FieldSet nonForwardedFields;
		
		private ImplicitlyForwardingSingleInputSemanticProperties(FieldSet nonForwardedFields) {
			this.nonForwardedFields = nonForwardedFields;
			addWrittenFields(nonForwardedFields);
		}
		
		
		/**
		 * Returns the logical position where the given field is written to.
		 * In this variant of the semantic properties, all fields are assumed implicitly forwarded,
		 * unless stated otherwise. We return the same field position, unless the field is explicitly
		 * marked as modified.
		 */
		@Override
		public FieldSet getForwardedField(int sourceField) {
			if (this.nonForwardedFields.contains(sourceField)) {
				return null;
			} else {
				return new FieldSet(sourceField);
			}
		}
		
		@Override
		public void addForwardedField(int sourceField, int destinationField) {
			throw new UnsupportedOperationException("When defining fields as implicitly constant " +
					"(such as through the ConstantFieldsExcept annotation), you cannot manually add forwarded fields.");
		}
		
		@Override
		public void addForwardedField(int sourceField, FieldSet destinationFields) {
			throw new UnsupportedOperationException("When defining fields as implicitly constant " +
					"(such as through the ConstantFieldsExcept annotation), you cannot manually add forwarded fields.");
		}
		
		@Override
		public void setForwardedField(int sourceField, FieldSet destinationFields) {
			throw new UnsupportedOperationException("When defining fields as implicitly constant " +
					"(such as through the ConstantFieldsExcept annotation), you cannot manually add forwarded fields.");
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
		public FieldSet getForwardedField1(int sourceField) {
			if (this.nonForwardedFields1 == null) {
				return super.getForwardedField1(sourceField);
			}
			else {
				if (this.nonForwardedFields1.contains(sourceField)) {
					return null;
				} else {
					return new FieldSet(sourceField);
				}
			}
		}
		
		@Override
		public FieldSet getForwardedField2(int sourceField) {
			if (this.nonForwardedFields2 == null) {
				return super.getForwardedField2(sourceField);
			}
			else {
				if (this.nonForwardedFields2.contains(sourceField)) {
					return null;
				} else {
					return new FieldSet(sourceField);
				}
			}
		}
		
		@Override
		public void addForwardedField1(int sourceField, int destinationField) {
			if (this.nonForwardedFields1 == null) {
				super.addForwardedField1(sourceField, destinationField);
			}
			else {
				throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
						"(such as through the ConstantFieldsFirstExcept annotation), you cannot manually add forwarded fields.");
			}
		}
		
		@Override
		public void addForwardedField1(int sourceField, FieldSet destinationFields) {
			if (this.nonForwardedFields1 == null) {
				super.addForwardedField1(sourceField, destinationFields);
			}
			else {
				throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
						"(such as through the ConstantFieldsFirstExcept annotation), you cannot manually add forwarded fields.");
			}
		}
		
		@Override
		public void setForwardedField1(int sourceField, FieldSet destinationFields) {
			if (this.nonForwardedFields1 == null) {
				super.addForwardedField1(sourceField, destinationFields);
			}
			else {
				throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
						"(such as through the ConstantFieldsFirstExcept annotation), you cannot manually add forwarded fields.");
			}
		}
		
		@Override
		public void addForwardedField2(int sourceField, int destinationField) {
			if (this.nonForwardedFields2 == null) {
				super.addForwardedField2(sourceField, destinationField);
			}
			else {
				throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
						"(such as through the ConstantFieldsSecondExcept annotation), you cannot manually add forwarded fields.");
			}
		}
		
		@Override
		public void addForwardedField2(int sourceField, FieldSet destinationFields) {
			if (this.nonForwardedFields2 == null) {
				super.addForwardedField2(sourceField, destinationFields);
			}
			else {
				throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
						"(such as through the ConstantFieldsSecondExcept annotation), you cannot manually add forwarded fields.");
			}
		}
		
		@Override
		public void setForwardedField2(int sourceField, FieldSet destinationFields) {
			if (this.nonForwardedFields2 == null) {
				super.addForwardedField2(sourceField, destinationFields);
			}
			else {
				throw new UnsupportedOperationException("When defining fields as implicitly constant for an input" +
						"(such as through the ConstantFieldsSecondExcept annotation), you cannot manually add forwarded fields.");
			}
		}
	}
}
