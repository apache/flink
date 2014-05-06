
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
package eu.stratosphere.api.java.functions;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Set;

import com.google.common.primitives.Ints;

import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.api.java.typeutils.TypeInformation;


/**
 * This class defines the semantic assertions that can be added to functions.
 * The assertions are realized as java annotations, to be added to the class declaration of
 * the class that realized the user function. For example, to declare the <i>ConstantFieldsExcept</i> 
 * annotation for a map-type function that realizes a simple absolute function,
 * use it the following way:
 * 
 * <pre><blockquote>
 * \@ConstantFieldsExcept(value={1,2}, outTuplePos={2,1})
 * public class MyMapper extends FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>
 * {
 *     public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple3<String, Integer, Integer>> out) {
			Integer tmp = value.f2;
			value.f2 = value.f1;
			value.f1 = tmp;
			out.collect(value);
		}
 * }
 * </blockquote></pre>
 * 
 * Be aware that some annotations should only be used for functions with as single input 
 * ({@link MapFunction}, {@link ReduceFunction}) and some only for stubs with two inputs 
 * ({@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction}).
 */
public class FunctionAnnotation {
	
	/**
	 * Specifies the fields of an input tuple or custom object that are unchanged in the output of 
	 * a stub with a single input ( {@link MapFunction}, {@link ReduceFunction}).
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * The annotation takes two int or String arrays. For correct use, one or two parameters should be set. The
	 * first array contains either integer positions of constant fields if tuples are used or the names of the fields
	 * for custom types. If only input positions are specified, it is assumed that the positions in the output remain identical. If
	 * a second parameter is set, it specifies the position of the values in the output data.
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
		String[] value();
	}
	
	/**
	 * Specifies the fields of an input tuple or custom object of the first input that are unchanged in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * The annotation takes two int or String arrays. For correct use, one or two parameters should be set. The
	 * first array contains either integer positions of constant fields if tuples are used or the names of the fields
	 * for custom types. If only input positions are specified, it is assumed that the positions in the output remain identical. If
	 * a second parameter is set, it specifies the position of the values in the output data.
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
		String[] value();
	}
	
	/**
	 * Specifies the fields of an input tuple or custom object of the second input that are unchanged in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * The annotation takes two int or String arrays. For correct use, one or two parameters should be set. The
	 * first array contains either integer positions of constant fields if tuples are used or the names of the fields
	 * for custom types. If only input positions are specified, it is assumed that the positions in the output remain identical. If
	 * a second parameter is set, it specifies the position of the values in the output data.
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
		String[] value();
	}
	
	/**
	 * Specifies the fields of an input tuple or custom object that are changed in the output of 
	 * a stub with a single input ( {@link MapFunction}, {@link ReduceFunction}). All other 
	 * fields are assumed to be constant.
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * The annotation takes one array specifying the positions of the input types that do not remain constant. This
	 * is possible for custom types using the 'inCustomPos' parameter and for tuples using the 'inTuplePos' parameter.
	 * When this annotation is used, it is assumed that all other values remain at the same position in input and output. To model
	 * more complex situations use the \@ConstantFields annotation.
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
		String[] value();
	}
	
	/**
	 * Specifies the fields of an input tuple or custom object of the first input that are changed in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * All other fields are assumed to be constant.
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * The annotation takes one array specifying the positions of the input types that do not remain constant. This
	 * is possible for custom types using the 'inCustomPos' parameter and for tuples using the 'inTuplePos' parameter.
	 * When this annotation is used, it is assumed that all other values remain at the same position in input and output. To model
	 * more complex situations use the \@ConstantFields annotation.
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
		String[] value();
	}
	
	
	/**
	 * Specifies the fields of an input tuple or custom object of the second input that are changed in 
	 * the output of a stub with two inputs ( {@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction})
	 * All other fields are assumed to be constant.
	 * 
	 * A field is considered to be constant if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * The annotation takes one array specifying the positions of the input types that do not remain constant. This
	 * is possible for custom types using the 'inCustomPos' parameter and for tuples using the 'inTuplePos' parameter.
	 * When this annotation is used, it is assumed that all other values remain at the same position in input and output. To model
	 * more complex situations use the \@ConstantFields annotation.
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
		String[] value();
	}
	
	/**
	 * Specifies the fields of an input tuple or custom object that are accessed in the function. This annotation should be used
	 * with user defined functions with one input.
	 */
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadFields {
		String[] value();
	}
	
	/**
	 * Specifies the fields of an input tuple or custom object that are accessed in the function. This annotation should be used
	 * with user defined functions with two inputs.
	 */
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadFieldsSecond {
		String[] value();
	}
	
	/**
	 * Specifies the fields of an input tuple or custom object that are accessed in the function. This annotation should be used
	 * with user defined functions with two inputs.
	 */
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadFieldsFirst {
		String[] value();
	}
	/**
	 * Private constructor to prevent instantiation. This class is intended only as a container.
	 */
	private FunctionAnnotation() {}
	
	// --------------------------------------------------------------------------------------------
	//                                   Function Annotation Handling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Reads the annotations of a user defined function with one input and returns semantic properties according to the constant fields annotated.
	 * @param udf	The user defined function.
	 * @param input	Type information of the operator input.
	 * @param output	Type information of the operator output.
	 * @return	The DualInputSemanticProperties containing the constant fields.
	 */
	
	public static Set<Annotation> readSingleConstantAnnotations(UserCodeWrapper<?> udf) {		
		ConstantFields constantSet = udf.getUserCodeAnnotation(ConstantFields.class);
		ConstantFieldsExcept notConstantSet = udf.getUserCodeAnnotation(ConstantFieldsExcept.class);
		ReadFields readfieldSet = udf.getUserCodeAnnotation(ReadFields.class);

		Set<Annotation> result = null;
		
		if (notConstantSet != null && constantSet != null) {
			throw new RuntimeException("Either ConstantFields or ConstantFieldsExcept can be specified, not both.");
		}
		
		if (notConstantSet != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(notConstantSet);
		}
		if (constantSet != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(constantSet);
		}
		
		if (readfieldSet != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(readfieldSet);
		}
		
		return result;
	}
	
	// --------------------------------------------------------------------------------------------
	/**
	 * Reads the annotations of a user defined function with two inputs and returns semantic properties according to the constant fields annotated.
	 * @param udf	The user defined function.
	 * @param input1	Type information of the first operator input.
	 * @param input2	Type information of the second operator input.
	 * @param output	Type information of the operator output.
	 * @return	The DualInputSemanticProperties containing the constant fields.
	 */
	
	public static Set<Annotation> readDualConstantAnnotations(UserCodeWrapper<?> udf) {

		// get readSet annotation from stub
		ConstantFieldsFirst constantSet1 = udf.getUserCodeAnnotation(ConstantFieldsFirst.class);
		ConstantFieldsSecond constantSet2= udf.getUserCodeAnnotation(ConstantFieldsSecond.class);
			
		// get readSet annotation from stub
		ConstantFieldsFirstExcept notConstantSet1 = udf.getUserCodeAnnotation(ConstantFieldsFirstExcept.class);
		ConstantFieldsSecondExcept notConstantSet2 = udf.getUserCodeAnnotation(ConstantFieldsSecondExcept.class);
			
		ReadFieldsFirst readfieldSet1 = udf.getUserCodeAnnotation(ReadFieldsFirst.class);
		ReadFieldsSecond readfieldSet2 = udf.getUserCodeAnnotation(ReadFieldsSecond.class);
		
		if (notConstantSet1 != null && constantSet1 != null) {
			throw new RuntimeException("Either ConstantFieldsFirst or ConstantFieldsFirstExcept can be specified, not both.");
		}
		
		if (constantSet2 != null && notConstantSet2 != null) {
			throw new RuntimeException("Either ConstantFieldsSecond or ConstantFieldsSecondExcept can be specified, not both.");
		}
		
		Set<Annotation> result = null;
		
		if (notConstantSet2 != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(notConstantSet2);
		}
		if (constantSet2 != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(constantSet2);
		}
		
		if (readfieldSet2 != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(readfieldSet2);
		}
		
		if (notConstantSet1 != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(notConstantSet1);
		}
		if (constantSet1 != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(constantSet1);
		}
		
		if (readfieldSet1 != null) {
			if (result == null) {
				result = new HashSet<Annotation>();
			}
			result.add(readfieldSet1);
		}
		
		return result;
	}
	
	
		
		
	}

