
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
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.Retention;
import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.api.common.InvalidProgramException;

/**
 * This class defines the semantic assertions that can be added to functions.
 * The assertions are realized as java annotations, to be added to the class declaration of
 * the class that implements the functions. For example, to declare the <i>ConstantFields</i>
 * annotation for a map-type function that simply copies some fields, use it the following way:
 *
 * <pre><blockquote>
 * \@ConstantFields({"0->0,1", "1->2"})
 * public class MyMapper extends FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, String, Integer>>
 * {
 *     public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple3<String, String, Integer>> out) {
 *         value.f2 = value.f1
 *         value.f1 = value.f0;
 *         out.collect(value);
 *     }
 * }
 * </blockquote></pre>
 * <p>
 * All annotations takes String arrays. The Strings represent the source and destination fields.
 * The transition is represented by the arrow "->".
 * Fields are described by their tuple position (and later also the names of the fields in the objects).
 * The left hand side of the arrow always describes the fields in the input value(s), i.e. the value that 
 * is passed as a parameter to the function call, or the values obtained from the input iterator. The right
 * hand side of the arrow describes the field in the value returned from the function. If the right hand side
 * is omitted, the a field is assumed to stay exactly the same, i.e. the field itself is unmodified, rather 
 * than that the value is placed into another field.
 * <p>
 * <b>
 * It is very important to follow a conservative strategy when specifying constant fields.
 * Only fields that are always constant (regardless of value, stub call, etc.) to the output may be
 * declared as such! Otherwise, the correct execution of a program can not be guaranteed. So if in doubt,
 * do not add a field to this set.
 * </b>
 * <p>
 * Be aware that some annotations should only be used for functions with as single input
 * ({@link MapFunction}, {@link ReduceFunction}) and some only for stubs with two inputs
 * ({@link CrossFunction}, {@link JoinFunction}, {@link CoGroupFunction}).
 */
public class FunctionAnnotation {

	/**
	 * This annotation declares that a function leaves certain fields of its input values unmodified and
	 * only "forwards" or "copies" them to the return value. The annotation is applicable to unary
	 * functions, like for example {@link MapFunction}, {@link ReduceFunction}, or {@link FlatMapFunction}.
	 * <p>
	 * The following example illustrates a function that keeps the tuple's field zero constant:
	 * <pre><blockquote>
	 * \@ConstantFields("0")
	 * public class MyMapper extends MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>
	 * {
	 *     public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> value) {
	 *         return new Tuple2<String, Double>(value.f0, value.f1 * 0.5);
	 *     }
	 * }
	 * </blockquote></pre>
	 * <p>
	 * (Note that you could equivalently write {@code @ConstantFields("0 -> 0")}.
	 * <p>
	 * This annotation is mutually exclusive with the {@link ConstantFieldsExcept} annotation.
	 * <p>
	 * If neither this annotation, nor the {@link ConstantFieldsExcept} annotation are set, it is
	 * assumed that <i>no</i> field in the input is forwarded/copied unmodified.
	 * 
	 * @see ConstantFieldsExcept
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFields {
		String[] value();
	}

	/**
	 * This annotation declares that a function leaves certain fields of its first input values unmodified and
	 * only "forwards" or "copies" them to the return value. The annotation is applicable to binary
	 * functions, like for example {@link JoinFunction}, {@link CoGroupFunction}, or {@link CrossFunction}.
	 * <p>
	 * The following example illustrates a join function that copies fields from the first and second input to the
	 * return value:
	 * <pre><blockquote>
	 * \@ConstantFieldsFirst("1 -> 0")
	 * \@ConstantFieldsFirst("1 -> 1")
	 * public class MyJoin extends JoinFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple2<Integer, String>>
	 * {
	 *     public Tuple2<Integer, String> map(Tuple2<String, Integer> first, Tuple2<String, String> second) {
	 *         return new Tuple2<String, Double>(first.f1, second.f1);
	 *     }
	 * }
	 * </blockquote></pre>
	 * <p>
	 * This annotation is mutually exclusive with the {@link ConstantFieldsFirstExcept} annotation.
	 * <p>
	 * If neither this annotation, nor the {@link ConstantFieldsFirstExcept} annotation are set, it is
	 * assumed that <i>no</i> field in the first input is forwarded/copied unmodified.
	 * 
	 * @see ConstantFieldsSecond
	 * @see ConstantFields
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsFirst {
		String[] value();
	}

	/**
	 * This annotation declares that a function leaves certain fields of its second input values unmodified and
	 * only "forwards" or "copies" them to the return value. The annotation is applicable to binary
	 * functions, like for example {@link JoinFunction}, {@link CoGroupFunction}, or {@link CrossFunction}.
	 * <p>
	 * The following example illustrates a join function that copies fields from the first and second input to the
	 * return value:
	 * <pre><blockquote>
	 * \@ConstantFieldsFirst("1 -> 0")
	 * \@ConstantFieldsFirst("1 -> 1")
	 * public class MyJoin extends JoinFunction<Tuple2<String, Integer>, Tuple2<String, String>, Tuple2<Integer, String>>
	 * {
	 *     public Tuple2<Integer, String> map(Tuple2<String, Integer> first, Tuple2<String, String> second) {
	 *         return new Tuple2<String, Double>(first.f1, second.f1);
	 *     }
	 * }
	 * </blockquote></pre>
	 * <p>
	 * This annotation is mutually exclusive with the {@link ConstantFieldsSecond} annotation.
	 * <p>
	 * If neither this annotation, nor the {@link ConstantFieldsSecondExcept} annotation are set, it is
	 * assumed that <i>no</i> field in the second input is forwarded/copied unmodified.
	 * 
	 * @see ConstantFieldsFirst
	 * @see ConstantFields
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsSecond {
		String[] value();
	}

	/**
	 * This annotation declares that a function changes certain fields of its input values, while leaving all
	 * others unmodified and in place in the return value. The annotation is applicable to unary
	 * functions, like for example {@link MapFunction}, {@link ReduceFunction}, or {@link FlatMapFunction}.
	 * <p>
	 * The following example illustrates that at the example of a Map function:
	 * 
	 * <pre><blockquote>
	 * \@ConstantFieldsExcept("1")
	 * public class MyMapper extends MapFunction<Tuple3<String, Integer, Double>, Tuple3<String, Double, Double>>
	 * {
	 *     public Tuple3<String, String, Double> map(Tuple3<String, Integer, Double> value) {
	 *         return new Tuple3<String, String, Double>(value.f0, value.f2 / 2, value.f2);
	 *     }
	 * }
	 * </blockquote></pre>
	 * <p>
	 * The annotation takes one String array specifying the positions of the input types that do not remain constant.
	 * When this annotation is used, it is assumed that all other values remain at the same position in input and output.
	 * To model more complex situations use the {@link @ConstantFields}s annotation.
	 * <p>
	 * This annotation is mutually exclusive with the {@link ConstantFields} annotation.
	 * <p>
	 * If neither this annotation, nor the {@link ConstantFields} annotation are set, it is
	 * assumed that <i>no</i> field in the input is forwarded/copied unmodified.
	 * 
	 * @see ConstantFieldsExcept
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsExcept {
		String[] value();
	}

	/**
	 * This annotation declares that a function changes certain fields of its first input value, while leaving all
	 * others unmodified and in place in the return value. The annotation is applicable to binary
	 * functions, like for example {@link JoinFunction}, {@link CoGroupFunction}, or {@link CrossFunction}.
	 * <p>
	 * The following example illustrates a join function that copies fields from the first and second input to the
	 * return value:
	 * 
	 * <pre><blockquote>
	 * \@ConstantFieldsFirstExcept("1")
	 * public class MyJoin extends JoinFunction<Tuple3<String, Integer, Double>, Tuple2<String, Double>, Tuple3<String, Double, Double>>
	 * {
	 *     public Tuple3<String, Double, Double> map(Tuple3<String, Integer, Double> first, Tuple2<String, Double> second) {
	 *         return Tuple3<String, Double, Double>(first.f0, second.f1, first.f2);
	 *     }
	 * }
	 * </blockquote></pre>
	 * <p>
	 * The annotation takes one String array specifying the positions of the input types that do not remain constant.
	 * When this annotation is used, it is assumed that all other values remain at the same position in input and output.
	 * To model more complex situations use the {@link @ConstantFields}s annotation.
	 * <p>
	 * This annotation is mutually exclusive with the {@link ConstantFieldsFirst}
	 * <p>
	 * If neither this annotation, nor the {@link ConstantFieldsFirst} annotation are set, it is
	 * assumed that <i>no</i> field in the first input is forwarded/copied unmodified.
	 * 
	 * @see ConstantFieldsFirst
	 * @see ConstantFieldsSecond
	 * @see ConstantFieldsSecondExcept
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsFirstExcept {
		String[] value();
	}

	/**
	 * This annotation declares that a function changes certain fields of its second input value, while leaving all
	 * others unmodified and in place in the return value. The annotation is applicable to binary
	 * functions, like for example {@link JoinFunction}, {@link CoGroupFunction}, or {@link CrossFunction}.
	 * <p>
	 * The following example illustrates a join function that copies fields from the first and second input to the
	 * return value:
	 * 
	 * <pre><blockquote>
	 * \@ConstantFieldsSecondExcept("1")
	 * public class MyJoin extends JoinFunction<Tuple2<String, Double>, Tuple3<String, Integer, Double>, Tuple3<String, Double, Double>>
	 * {
	 *     public Tuple3<String, Double, Double> map(Tuple2<String, Double> first, Tuple3<String, Integer, Double> second) {
	 *         return Tuple3<String, Double, Double>(second.f0, first.f1, second.f2);
	 *     }
	 * }
	 * </blockquote></pre>
	 * <p>
	 * The annotation takes one String array specifying the positions of the input types that do not remain constant.
	 * When this annotation is used, it is assumed that all other values remain at the same position in input and output.
	 * To model more complex situations use the {@link @ConstantFields}s annotation.
	 * <p>
	 * This annotation is mutually exclusive with the {@link ConstantFieldsSecond}
	 * <p>
	 * If neither this annotation, nor the {@link ConstantFieldsSecond} annotation are set, it is
	 * assumed that <i>no</i> field in the second input is forwarded/copied unmodified.
	 * 
	 * @see ConstantFieldsFirst
	 * @see ConstantFieldsFirstExcept
	 * @see ConstantFieldsSecond
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsSecondExcept {
		String[] value();
	}

	/**
	 * Specifies the fields of the input value of a user-defined that are accessed in the code.
	 * This annotation can only be used with user-defined functions with one input (Map, Reduce, ...).
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadFields {
		String[] value();
	}

	/**
	 * Specifies the fields of the first input value of a user-defined that are accessed in the code.
	 * This annotation can only be used with user-defined functions with two inputs (Join, Cross, ...).
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadFieldsSecond {
		String[] value();
	}

	/**
	 * Specifies the fields of the second input value of a user-defined that are accessed in the code.
	 * This annotation can only be used with user-defined functions with two inputs (Join, Cross, ...).
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
	 * 
	 * @param udfClass The user defined function, represented by its class.
	 * @return	The DualInputSemanticProperties containing the constant fields.
	 */
	public static Set<Annotation> readSingleConstantAnnotations(Class<?> udfClass) {
		ConstantFields constantSet = udfClass.getAnnotation(ConstantFields.class);
		ConstantFieldsExcept notConstantSet = udfClass.getAnnotation(ConstantFieldsExcept.class);
		ReadFields readfieldSet = udfClass.getAnnotation(ReadFields.class);

		Set<Annotation> result = null;

		if (notConstantSet != null && constantSet != null) {
			throw new InvalidProgramException("Either " + ConstantFields.class.getSimpleName() + " or " + 
					ConstantFieldsExcept.class.getSimpleName() + " can be annotated to a function, not both.");
		}

		if (notConstantSet != null) {
			result = new HashSet<Annotation>();
			result.add(notConstantSet);
		}
		if (constantSet != null) {
			result = new HashSet<Annotation>();
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
	 * @param udfClass The user defined function, represented by its class.
	 * @return	The DualInputSemanticProperties containing the constant fields.
	 */

	public static Set<Annotation> readDualConstantAnnotations(Class<?> udfClass) {

		// get readSet annotation from stub
		ConstantFieldsFirst constantSet1 = udfClass.getAnnotation(ConstantFieldsFirst.class);
		ConstantFieldsSecond constantSet2= udfClass.getAnnotation(ConstantFieldsSecond.class);

		// get readSet annotation from stub
		ConstantFieldsFirstExcept notConstantSet1 = udfClass.getAnnotation(ConstantFieldsFirstExcept.class);
		ConstantFieldsSecondExcept notConstantSet2 = udfClass.getAnnotation(ConstantFieldsSecondExcept.class);

		ReadFieldsFirst readfieldSet1 = udfClass.getAnnotation(ReadFieldsFirst.class);
		ReadFieldsSecond readfieldSet2 = udfClass.getAnnotation(ReadFieldsSecond.class);

		if (notConstantSet1 != null && constantSet1 != null) {
			throw new InvalidProgramException("Either " + ConstantFieldsFirst.class.getSimpleName() + " or " + 
					ConstantFieldsFirstExcept.class.getSimpleName() + " can be annotated to a function, not both.");
		}

		if (constantSet2 != null && notConstantSet2 != null) {
			throw new InvalidProgramException("Either " + ConstantFieldsSecond.class.getSimpleName() + " or " + 
					ConstantFieldsSecondExcept.class.getSimpleName() + " can be annotated to a function, not both.");
		}

		Set<Annotation> result = null;

		if (notConstantSet2 != null) {
			result = new HashSet<Annotation>();
			result.add(notConstantSet2);
		}
		if (constantSet2 != null) {
			result = new HashSet<Annotation>();
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

