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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.InvalidProgramException;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Set;

/**
 * This class defines Java annotations for semantic assertions that can be added to Flink functions.
 * Semantic annotations can help the Flink optimizer to generate more efficient execution plans for Flink programs.
 * For example, a <i>ForwardedFields</i> assertion for a map-type function can be declared as:
 *
 * <pre>{@code
 * {@literal @}ForwardedFields({"f0; f2->f1"})
 * public class MyMapper extends MapFunction<Tuple3<String, String, Integer>, Tuple3<String, Integer, Integer>>
 * {
 *     public Tuple3<String, Integer, Integer> map(Tuple3<String, String, Integer> val) {
 *
 *         return new Tuple3<String, Integer, Integer>(val.f0, val.f2, 1);
 *     }
 * }
 * }</pre>
 *
 *
 * <p>All annotations take Strings with expressions that refer to (nested) value fields of the input and output types of a function.
 * Field expressions for of composite data types (tuples, POJOs, Scala case classes) can be expressed in
 * different ways, depending on the data type they refer to.
 *
 * <ul>
 *     <li>Java tuple data types (such as {@link org.apache.flink.api.java.tuple.Tuple3}): A tuple field can be addressed using
 * its 0-offset index or name, e.g., the second field of a Java tuple is addressed by <code>"1"</code> or <code>"f1"</code>.</li>
 *     <li>Java POJO data types: A POJO field is addressed using its names, e.g., <code>"xValue"</code> for the member field
 * <code>xValue</code> of a POJO type that describes a 2d-coordinate.</li>
 *     <li>Scala tuple data types (such as {@link scala.Tuple3}): A tuple field can be addressed using its 1-offset name
 * (following Scala conventions) or 0-offset index, e.g., the second field of a Scala tuple is addressed by
 * <code>"_2"</code> or <code>1</code></li>
 *     <li>Scala case classes: A case class field is addressed using its names, e.g., <code>"xValue"</code> for the field <code>xValue</code>
 * of a case class that describes a 2d-coordinate.</li>
 * </ul>
 *
 *
 * <p>Nested fields are addressed by navigation, e.g., <code>"f1.xValue"</code> addresses the field <code>xValue</code> of a POJO type,
 * that is stored at the second field of a Java tuple. In order to refer to all fields of a composite type (or the composite type itself)
 * such as a tuple, POJO, or case class type, a <code>"*"</code> wildcard can be used, e.g., <code>f2.*</code> or <code>f2</code> reference all fields
 * of a composite type at the third position of a Java tuple.
 *
 * <p><b>NOTE: The use of semantic annotation is optional!
 * If used correctly, semantic annotations can help the Flink optimizer to generate more efficient execution plans.
 * However, incorrect semantic annotations can cause the optimizer to generate incorrect execution plans which compute wrong results!
 * So be careful when adding semantic annotations.
 * </b>
 *
 */
@Public
public class FunctionAnnotation {

	/**
	 * The ForwardedFields annotation declares fields which are never modified by the annotated function and
	 * which are forwarded at the same position to the output or unchanged copied to another position in the output.
	 *
	 * <p>Fields that are forwarded at the same position can be specified by their position.
	 * The specified position must be valid for the input and output data type and have the same type.
	 * For example {@code {@literal @}ForwardedFields({"f2"})} declares that the third field of a Java input tuple is
	 * copied to the third field of an output tuple.
	 *
	 * <p>Fields which are unchanged copied to another position in the output are declared by specifying the
	 * source field expression in the input and the target field expression in the output.
	 * {@code {@literal @}ForwardedFields({"f0->f2"})} denotes that the first field of the Java input tuple is
	 * unchanged copied to the third field of the Java output tuple. When using the wildcard ("*") ensure that
	 * the number of declared fields and their types in input and output type match.
	 *
	 * <p>Multiple forwarded fields can be annotated in one ({@code {@literal @}ForwardedFields({"f2; f3->f0; f4"})})
	 * or separate Strings ({@code {@literal @}ForwardedFields({"f2", "f3->f0", "f4"})}).
	 *
	 * <p><b>NOTE: The use of the ForwardedFields annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * However if used incorrectly, it can cause invalid plan choices and the computation of wrong results!
	 * It is NOT required that all forwarded fields are declared, but all declarations must be correct.
	 * </b>
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ForwardedFields {
		String[] value();
	}

	/**
	 * The ForwardedFieldsFirst annotation declares fields of the first input of a function which are
	 * never modified by the annotated function and which are forwarded at the same position to the
	 * output or unchanged copied to another position in the output.
	 *
	 * <p>Fields that are forwarded from the first input at the same position in the output can be
	 * specified by their position. The specified position must be valid for the input and output data type and have the same type.
	 * For example {@code {@literal @}ForwardedFieldsFirst({"f2"})} declares that the third field of a Java input tuple at the first input is
	 * copied to the third field of an output tuple.
	 *
	 * <p>Fields which are unchanged copied to another position in the output are declared by specifying the
	 * source field expression in the input and the target field expression in the output.
	 * {@code {@literal @}ForwardedFieldsFirst({"f0->f2"})} denotes that the first field of the Java input tuple at the first input is
	 * unchanged copied to the third field of the Java output tuple. When using the wildcard ("*") ensure that
	 * the number of declared fields and their types in input and output type match.
	 *
	 * <p>Multiple forwarded fields can be annotated in one ({@code {@literal @}ForwardedFieldsFirst({"f2; f3->f0; f4"})})
	 * or separate Strings ({@code {@literal @}ForwardedFieldsFirst({"f2", "f3->f0", "f4"})}).
	 *
	 * <p><b>NOTE: The use of the ForwardedFieldsFirst annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * However if used incorrectly, it can cause invalid plan choices and the computation of wrong results!
	 * It is NOT required that all forwarded fields are declared, but all declarations must be correct.
	 * </b>
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 * <p>Forwarded fields from the second input can be specified using the
	 * {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond} annotation.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ForwardedFieldsFirst {
		String[] value();
	}

	/**
	 * The ForwardedFieldsSecond annotation declares fields of the second input of a function which are
	 * never modified by the annotated function and which are forwarded at the same position to the
	 * output or unchanged copied to another position in the output.
	 *
	 * <p>Fields that are forwarded from the second input at the same position in the output can be
	 * specified by their position. The specified position must be valid for the input and output data type and have the same type.
	 * For example {@code {@literal @}ForwardedFieldsSecond({"f2"})} declares that the third field of a Java input tuple at the second input is
	 * copied to the third field of an output tuple.
	 *
	 * <p>Fields which are unchanged copied to another position in the output are declared by specifying the
	 * source field expression in the input and the target field expression in the output.
	 * {@code {@literal @}ForwardedFieldsSecond({"f0->f2"})} denotes that the first field of the Java input tuple at the second input is
	 * unchanged copied to the third field of the Java output tuple. When using the wildcard ("*") ensure that
	 * the number of declared fields and their types in input and output type match.
	 *
	 * <p>Multiple forwarded fields can be annotated in one ({@code {@literal @}ForwardedFieldsSecond({"f2; f3->f0; f4"})})
	 * or separate Strings ({@code {@literal @}ForwardedFieldsSecond({"f2", "f3->f0", "f4"})}).
	 *
	 * <p><b>NOTE: The use of the ForwardedFieldsSecond annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * However if used incorrectly, it can cause invalid plan choices and the computation of wrong results!
	 * It is NOT required that all forwarded fields are declared, but all declarations must be correct.
	 * </b>
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 * <p>Forwarded fields from the first input can be specified using the
	 * {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst} annotation.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ForwardedFieldsSecond {
		String[] value();
	}

	/**
	 * The NonForwardedFields annotation declares ALL fields which not preserved on the same position in a functions output.
	 * ALL other fields are considered to be unmodified at the same position.
	 * Hence, the NonForwardedFields annotation is inverse to the {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields} annotation.
	 *
	 * <p><b>NOTE: The use of the NonForwardedFields annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * However if used incorrectly, it can cause invalid plan choices and the computation of wrong results!
	 * Since all not declared fields are considered to be forwarded, it is required that ALL non-forwarded fields are declared.
	 * </b>
	 *
	 * <p>Non-forwarded fields are declared as a list of field expressions, e.g., <code>\@NonForwardedFields({"f1; f3"})</code>
	 * declares that the second and fourth field of a Java tuple are modified and all other fields are are not changed and remain
	 * on their position. A NonForwardedFields annotation can only be used on functions where the type of the input and output are identical.
	 *
	 * <p>Multiple non-forwarded fields can be annotated in one (<code>\@NonForwardedFields({"f1; f3"})</code>)
	 * or separate Strings (<code>\@NonForwardedFields({"f1", "f3"})</code>).
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface NonForwardedFields {
		String[] value();
	}

	/**
	 * The NonForwardedFieldsFirst annotation declares for a function ALL fields of its first input
	 * which are not preserved on the same position in its output.
	 * ALL other fields are considered to be unmodified at the same position.
	 * Hence, the NonForwardedFieldsFirst annotation is inverse to the {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst} annotation.
	 *
	 * <p><b>NOTE: The use of the NonForwardedFieldsFirst annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * However if used incorrectly, it can cause invalid plan choices and the computation of wrong results!
	 * Since all not declared fields are considered to be forwarded, it is required that ALL non-forwarded fields of the first input are declared.
	 * </b>
	 *
	 * <p>Non-forwarded fields are declared as a list of field expressions, e.g., <code>\@NonForwardedFieldsFirst({"f1; f3"})</code>
	 * declares that the second and fourth field of a Java tuple from the first input are modified and
	 * all other fields of the first input are are not changed and remain on their position.
	 * A NonForwardedFieldsFirst annotation can only be used on functions where the type of the first input and the output are identical.
	 *
	 * <p>Multiple non-forwarded fields can be annotated in one (<code>\@NonForwardedFieldsFirst({"f1; f3"})</code>)
	 * or separate Strings (<code>\@NonForwardedFieldsFirst({"f1", "f3"})</code>).
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.NonForwardedFields
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface NonForwardedFieldsFirst {
		String[] value();
	}

	/**
	 * The NonForwardedFieldsSecond annotation declares for a function ALL fields of its second input
	 * which are not preserved on the same position in its output.
	 * ALL other fields are considered to be unmodified at the same position.
	 * Hence, the NonForwardedFieldsSecond annotation is inverse to the {@link org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond} annotation.
	 *
	 * <p><b>NOTE: The use of the NonForwardedFieldsSecond annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * However if used incorrectly, it can cause invalid plan choices and the computation of wrong results!
	 * Since all not declared fields are considered to be forwarded, it is required that ALL non-forwarded fields of the second input are declared.
	 * </b>
	 *
	 * <p>Non-forwarded fields are declared as a list of field expressions, e.g., <code>\@NonForwardedFieldsSecond({"f1; f3"})</code>
	 * declares that the second and fourth field of a Java tuple from the second input are modified and
	 * all other fields of the second input are are not changed and remain on their position.
	 * A NonForwardedFieldsSecond annotation can only be used on functions where the type of the second input and the output are identical.
	 *
	 * <p>Multiple non-forwarded fields can be annotated in one (<code>\@NonForwardedFieldsSecond({"f1; f3"})</code>)
	 * or separate Strings (<code>\@NonForwardedFieldsSecond({"f1", "f3"})</code>).
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.NonForwardedFields
	 * @see org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface NonForwardedFieldsSecond {
		String[] value();
	}

	/**
	 * The ReadFields annotation declares for a function all fields which it accesses and evaluates, i.e.,
	 * all fields that are used by the function to compute its result.
	 * For example, fields which are evaluated in conditional statements or used for computations are considered to be read.
	 * Fields which are only unmodified copied to the output without evaluating their values are NOT considered to be read.
	 *
	 * <p><b>NOTE: The use of the ReadFields annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * The ReadFields annotation requires that ALL read fields are declared.
	 * Otherwise, it can cause invalid plan choices and the computation of wrong results!
	 * Declaring a non-read field as read is not harmful but might reduce optimization potential.
	 * </b>
	 *
	 * <p>Read fields are declared as a list of field expressions, e.g., <code>\@ReadFields({"f0; f2"})</code> declares the first and third
	 * field of a Java input tuple to be read. All other fields are considered to not influence the behavior of the function.
	 *
	 * <p>Multiple read fields can be declared in one <code>\@ReadFields({"f0; f2"})</code> or
	 * multiple separate Strings <code>\@ReadFields({"f0", "f2"})</code>.
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	@PublicEvolving
	public @interface ReadFields {
		String[] value();
	}

	/**
	 * The ReadFieldsFirst annotation declares for a function all fields of the first input which it accesses and evaluates, i.e.,
	 * all fields of the first input that are used by the function to compute its result.
	 * For example, fields which are evaluated in conditional statements or used for computations are considered to be read.
	 * Fields which are only unmodified copied to the output without evaluating their values are NOT considered to be read.
	 *
	 * <p><b>NOTE: The use of the ReadFieldsFirst annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * The ReadFieldsFirst annotation requires that ALL read fields of the first input are declared.
	 * Otherwise, it can cause invalid plan choices and the computation of wrong results!
	 * Declaring a non-read field as read is not harmful but might reduce optimization potential.
	 * </b>
	 *
	 * <p>Read fields are declared as a list of field expressions, e.g., <code>\@ReadFieldsFirst({"f0; f2"})</code> declares the first and third
	 * field of a Java input tuple of the first input to be read.
	 * All other fields of the first input are considered to not influence the behavior of the function.
	 *
	 * <p>Multiple read fields can be declared in one <code>\@ReadFieldsFirst({"f0; f2"})</code> or
	 * multiple separate Strings <code>\@ReadFieldsFirst({"f0", "f2"})</code>.
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	@PublicEvolving
	public @interface ReadFieldsFirst {
		String[] value();
	}

	/**
	 * The ReadFieldsSecond annotation declares for a function all fields of the second input which it accesses and evaluates, i.e.,
	 * all fields of the second input that are used by the function to compute its result.
	 * For example, fields which are evaluated in conditional statements or used for computations are considered to be read.
	 * Fields which are only unmodified copied to the output without evaluating their values are NOT considered to be read.
	 *
	 * <p><b>NOTE: The use of the ReadFieldsSecond annotation is optional.
	 * If used correctly, it can help the Flink optimizer to generate more efficient execution plans.
	 * The ReadFieldsSecond annotation requires that ALL read fields of the second input are declared.
	 * Otherwise, it can cause invalid plan choices and the computation of wrong results!
	 * Declaring a non-read field as read is not harmful but might reduce optimization potential.
	 * </b>
	 *
	 * <p>Read fields are declared as a list of field expressions, e.g., <code>\@ReadFieldsSecond({"f0; f2"})</code> declares the first and third
	 * field of a Java input tuple of the second input to be read.
	 * All other fields of the second input are considered to not influence the behavior of the function.
	 *
	 * <p>Multiple read fields can be declared in one <code>\@ReadFieldsSecond({"f0; f2"})</code> or
	 * multiple separate Strings <code>\@ReadFieldsSecond({"f0", "f2"})</code>.
	 *
	 * <p>Please refer to the JavaDoc of {@link org.apache.flink.api.common.functions.Function} or Flink's documentation for
	 * details on field expressions such as nested fields and wildcard.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	@PublicEvolving
	public @interface ReadFieldsSecond {
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
	 * Reads the annotations of a user defined function with one input and returns semantic properties according to the forwarded fields annotated.
	 *
	 * @param udfClass The user defined function, represented by its class.
	 * @return	The DualInputSemanticProperties containing the forwarded fields.
	 */
	@Internal
	public static Set<Annotation> readSingleForwardAnnotations(Class<?> udfClass) {
		ForwardedFields forwardedFields = udfClass.getAnnotation(ForwardedFields.class);
		NonForwardedFields nonForwardedFields = udfClass.getAnnotation(NonForwardedFields.class);
		ReadFields readSet = udfClass.getAnnotation(ReadFields.class);

		Set<Annotation> annotations = new HashSet<Annotation>();
		if (forwardedFields != null) {
			annotations.add(forwardedFields);
		}
		if (nonForwardedFields != null) {
			if (!annotations.isEmpty()) {
				throw new InvalidProgramException("Either " + ForwardedFields.class.getSimpleName() + " or " +
						NonForwardedFields.class.getSimpleName() + " can be annotated to a function, not both.");
			}
			annotations.add(nonForwardedFields);
		}
		if (readSet != null) {
			annotations.add(readSet);
		}

		return !annotations.isEmpty() ? annotations : null;
	}

	// --------------------------------------------------------------------------------------------
	/**
	 * Reads the annotations of a user defined function with two inputs and returns semantic properties according to the forwarded fields annotated.
	 * @param udfClass The user defined function, represented by its class.
	 * @return	The DualInputSemanticProperties containing the forwarded fields.
	 */
	@Internal
	public static Set<Annotation> readDualForwardAnnotations(Class<?> udfClass) {

		// get readSet annotation from stub
		ForwardedFieldsFirst forwardedFields1 = udfClass.getAnnotation(ForwardedFieldsFirst.class);
		ForwardedFieldsSecond forwardedFields2 = udfClass.getAnnotation(ForwardedFieldsSecond.class);

		// get readSet annotation from stub
		NonForwardedFieldsFirst nonForwardedFields1 = udfClass.getAnnotation(NonForwardedFieldsFirst.class);
		NonForwardedFieldsSecond nonForwardedFields2 = udfClass.getAnnotation(NonForwardedFieldsSecond.class);

		ReadFieldsFirst readSet1 = udfClass.getAnnotation(ReadFieldsFirst.class);
		ReadFieldsSecond readSet2 = udfClass.getAnnotation(ReadFieldsSecond.class);

		Set<Annotation> annotations = new HashSet<Annotation>();

		if (nonForwardedFields1 != null && forwardedFields1 != null) {
			throw new InvalidProgramException("Either " + ForwardedFieldsFirst.class.getSimpleName() + " or " +
					NonForwardedFieldsFirst.class.getSimpleName() + " can be annotated to a function, not both.");
		} else if (forwardedFields1 != null) {
			annotations.add(forwardedFields1);
		} else if (nonForwardedFields1 != null) {
			annotations.add(nonForwardedFields1);
		}

		if (forwardedFields2 != null && nonForwardedFields2 != null) {
			throw new InvalidProgramException("Either " + ForwardedFieldsSecond.class.getSimpleName() + " or " +
					NonForwardedFieldsSecond.class.getSimpleName() + " can be annotated to a function, not both.");
		} else if (forwardedFields2 != null) {
			annotations.add(forwardedFields2);
		} else if (nonForwardedFields2 != null) {
			annotations.add(nonForwardedFields2);
		}

		if (readSet1 != null) {
			annotations.add(readSet1);
		}
		if (readSet2 != null) {
			annotations.add(readSet2);
		}

		return !annotations.isEmpty() ? annotations : null;
	}
}

