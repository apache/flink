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

package eu.stratosphere.pact.common.stubs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import eu.stratosphere.pact.common.type.PactRecord;


/**
 * This class defines the PACT annotations, realized as java annotations. To use
 * a PACT annotation, simply add the annotation above the class declaration of
 * the class that realized the user function. For example, to declare the <i>ReadSet</i> 
 * annotation for a map-type function that realizes a simple filter,
 * use it the following way:
 * 
 * <pre><blockquote>
 * \@ReadSet(fields={2})
 * public class MyMapper extends MapStub
 * {
 *     public void map(PactRecord record, Collector out)
 *     {
 *        String s = record.getField(2, PactString.class).toString();
 *        
 *        if (s.contains("...some text...") {
 *            out.collect(record);
 *        }Reduce
 *     }
 * }
 * </blockquote></pre>
 * 
 * Be aware that some annotations should only be used for stubs with as single input 
 * ({@link MapStub}, {@link ReduceStub}) and some only for stubs with two inputs 
 * ({@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class StubAnnotation
{

//	/**
//	 * Specifies the read set for a stub with a single input ( {@link MapStub}, {@link ReduceStub}).
//	 * The {@link Reads#fields()} attribute of the read set specifies all fields of the record 
//	 * that the stub reads and uses to modify its output. 
//	 * This is the case for if the value is used in conditional statements or to compute new values.
//	 * 
//	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
//	 * correct execution of PACT programs. 
//	 * 
//	 * If no ReadSet is specified, all fields are considered to be read.
//	 *
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface Reads
//	{
//		int[] fields();
//	}
//	
//	/**
//	 * Specifies the read set for the first input of a stub with two inputs ( {@link CrossStub}, 
//	 * {@link MatchStub}, {@link CoGroupStub}).
//	 * The {@link ReadsFirst#fields()} attribute of the read set specifies all fields of the record 
//	 * that the stub reads from the first input's records and uses to modify its output. 
//	 * This is the case for if the value is used in conditional statements or to compute new values.   
//	 *
//	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
//	 * correct execution of PACT programs. 
//	 *
//	 * If no ReadSet is specified, all fields are considered to be read.
//	 * 
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ReadsFirst
//	{
//		int[] fields();
//	}
//	
//	/**
//	 * Specifies the read set for the second input of a stub with two inputs ( {@link CrossStub}, 
//	 * {@link MatchStub}, {@link CoGroupStub}).
//	 * The {@link ReadsSecond#fields()} attribute of the read set specifies all fields of the record 
//	 * that the stub reads from the second input's records and uses to modify its output. 
//	 * This is the case for if the value is used in conditional statements or to compute new values.   
//	 *
//	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
//	 * correct execution of PACT programs. 
//	 * 
//	 * If no ReadSet is specified, all fields are considered to be read.
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ReadsSecond
//	{
//		int[] fields();
//	}
//	
//	/**
//	 * Specifies the implicit operation that is applied to the fields of input records 
//	 * of stub with a single input ( {@link MapStub}, {@link ReduceStub}).
//	 * Fields can either be implicitly copied or projected.
//	 * 
//	 * Implicit copying happens if all fields are copied to the output of the stub. For example, this is the 
//	 * case if input records are (modified) and emitted by the stub. It is important to notice
//	 * that implicit copy requires that <i>by default all</i> fields of an input record are emitted.
//	 * 
//	 * In contrast, implicit projection happens if no field is copied to the output. For example, this is the
//	 * case if a new {@link PactRecord} is created and emitted by the stub. 
//	 * 
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ImplicitOperation
//	{
//		public static enum ImplicitOperationMode {Copy, Projection};
//		
//		ImplicitOperationMode implicitOperation();
//	}
//	
//	/**
//	 * Specifies the implicit operation that is applied to the fields of input records of the first input 
//	 * of stub with two inputs ( {@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}).
//	 * Fields can either be implicitly copied or projected.
//	 * 
//	 * Implicit copying happens if all fields of the input are copied to the output of the stub. 
//	 * For example, this is the case if input records are (modified) and emitted by the stub. 
//	 * It is important to notice that implicit copy requires that <i>by default all</i> fields 
//	 * of an input record are emitted.
//	 * 
//	 * In contrast, implicit projection happens if no field of the input is copied to the output. 
//	 * For example, this is the case if a new {@link PactRecord} is created and emitted by the stub. 
//	 * 
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ImplicitOperationFirst
//	{
//		ImplicitOperationMode implicitOperation();
//	}
//	
//	/**
//	 * Specifies the implicit operation that is applied to the fields of input records of the second input 
//	 * of stub with two inputs ( {@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}).
//	 * Fields can either be implicitly copied or projected.
//	 * 
//	 * Implicit copying happens if all fields of the input are copied to the output of the stub. 
//	 * For example, this is the case if input records are (modified) and emitted by the stub. 
//	 * It is important to notice that implicit copy requires that <i>by default all</i> fields 
//	 * of an input record are emitted.
//	 * 
//	 * In contrast, implicit projection happens if no field of the input is copied to the output. 
//	 * For example, this is the case if a new {@link PactRecord} is created and emitted by the stub. 
//	 * 
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ImplicitOperationSecond
//	{
//		ImplicitOperationMode implicitOperation();
//	}
	
	/**
	 * Specifies the fields of an input record that are explicitly copied to the output of 
	 * a stub with a single input ( {@link MapStub}, {@link ReduceStub}).
	 * This annotation should be used and is only interpreted if the {@link ImplicitOperation} of the 
	 * input is set to {@link ImplicitOperation.ImplicitOperationMode#Projection}.
	 * 
	 * Since the default behavior is <i>projection</i> this annotation specifies which fields are 
	 * explicitly copied to records that are emitted by the stub.
	 * A field is considered to be copied if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying explicit copies.
	 * Only fields that are copied always (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a PACT program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 * 
	 * If this annotation is not set, it is assumed that <i>no</i> field is copied.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFields
	{
		int[] fields();
	}
	
	/**
	 * Specifies the fields of an input record that are explicitly copied from the first input of  
	 * a stub with two inputs ( {@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}) to the output.
	 * This annotation should be used and is only interpreted if the {@link ImplicitOperation} of the 
	 * input is set to {@link ImplicitOperation.ImplicitOperationMode#Projection}.
	 * 
	 * Since in this case, the default behavior is <i>projection</i> this annotation specifies which fields are 
	 * explicitly copied to records that are emitted by the stub.
	 * A field is considered to be copied if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying explicit copies.
	 * Only fields that are copied always (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a PACT program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 * 
	 * If this annotation is not set, it is assumed that <i>no</i> field is copied.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsFirst
	{
		int[] fields();
	}
	
	/**
	 * Specifies the fields of an input record that are explicitly copied from the second input of  
	 * a stub with two inputs ( {@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}) to the output.
	 * This annotation should be used and is only interpreted if the {@link ImplicitOperation} of the 
	 * input is set to {@link ImplicitOperation.ImplicitOperationMode#Projection}.
	 * 
	 * Since in this case, the default behavior is <i>projection</i> this annotation specifies which fields are 
	 * explicitly copied to records that are emitted by the stub.
	 * A field is considered to be copied if its value is not changed and copied to the same position of 
	 * output record.
	 * 
	 * <b>
	 * It is very important to follow a conservative strategy when specifying explicit copies.
	 * Only fields that are copied always (regardless of value, stub call, etc.) to the output may be 
	 * inserted! Otherwise, the correct execution of a PACT program can not be guaranteed.
	 * So if in doubt, do not add a field to this set.
	 * </b>
	 * 
	 * If this annotation is not set, it is assumed that <i>no</i> field is copied.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ConstantFieldsSecond
	{
		int[] fields();
	}
//	
//	/**
//	 * Specifies the fields of an input record that are explicitly projected and not contained in the output of 
//	 * a stub with a single input ( {@link MapStub}, {@link ReduceStub}).
//	 * This annotation should be used and is only interpreted if the {@link ImplicitOperation} of the 
//	 * input is set to {@link ImplicitOperation.ImplicitOperationMode#Copy}.
//	 * 
//	 * Since the default behavior is <i>copy</i> this annotation specifies which fields are 
//	 * explicitly projected and not contained in records that are emitted by the stub.
//	 * A field is considered to be projected if the field of the output record is explicitly set to <i>null</i>.
//	 * 
//	 * <b>
//	 * It is very important to follow a conservative strategy when specifying explicit copies.
//	 * Every field that is projected in at least on output record must be inserted! 
//	 * Otherwise, the correct execution of a PACT program can not be guaranteed.
//	 * So if in doubt, add a field to the set of projected fields.
//	 * </b>
//	 * 
//	 * If this annotation is not set, it is assumed that <i>all</i> fields are projected.
//	 *
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ExplicitProjections
//	{
//		int[] fields();
//	}
//	
//	/**
//	 * Specifies the fields of an input record of the first input of a stub with two inputs 
//	 * ({@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}) that are explicitly projected 
//	 * and not contained in the output.
//	 * This annotation should be used and is only interpreted if the {@link ImplicitOperation} of the 
//	 * input is set to {@link ImplicitOperation.ImplicitOperationMode#Copy}.
//	 * 
//	 * Since the default behavior is <i>copy</i> this annotation specifies which fields are 
//	 * explicitly projected and not contained in records that are emitted by the stub.
//	 * A field is considered to be projected if the field of the output record is explicitly set to <i>null</i>.
//	 * 
//	 * <b>
//	 * It is very important to follow a conservative strategy when specifying explicit copies.
//	 * Every field that is projected in at least on output record must be inserted! 
//	 * Otherwise, the correct execution of a PACT program can not be guaranteed.
//	 * So if in doubt, add a field to the set of projected fields.
//	 * </b>
//	 * 
//	 * If this annotation is not set, it is assumed that <i>all</i> fields are projected.
//	 *
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ExplicitProjectionsFirst
//	{
//		int[] fields();
//	}
//	
//	/**
//	 * Specifies the fields of an input record of the second input of a stub with two inputs 
//	 * ({@link CrossStub}, {@link MatchStub}, {@link CoGroupStub}) that are explicitly projected 
//	 * and not contained in the output.
//	 * This annotation should be used and is only interpreted if the {@link ImplicitOperation} of the 
//	 * input is set to {@link ImplicitOperation.ImplicitOperationMode#Copy}.
//	 * 
//	 * Since the default behavior is <i>copy</i> this annotation specifies which fields are 
//	 * explicitly projected and not contained in records that are emitted by the stub.
//	 * A field is considered to be projected if the field of the output record is explicitly set to <i>null</i>.
//	 * 
//	 * <b>
//	 * It is very important to follow a conservative strategy when specifying explicit copies.
//	 * Every field that is projected in at least on output record must be inserted! 
//	 * Otherwise, the correct execution of a PACT program can not be guaranteed.
//	 * So if in doubt, add a field to the set of projected fields.
//	 * </b>
//	 * 
//	 * If this annotation is not set, it is assumed that <i>all</i> fields are projected.
//	 *
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ExplicitProjectionsSecond
//	{
//		int[] fields();
//	}
//
//	/**
//	 * The {@link ExplicitModifications} annotation contains all fields of the output record 
//	 * which have been explicitly modified. 
//	 * 
//	 * An explicit modification is done by calling the 
//	 * {@link PactRecord#setField(int, eu.stratosphere.pact.common.type.Value)} method.
//	 * Setting a field to <i>null</i> is considered to be a projection and 
//	 * <b>not</b> a modification.
//	 * 
//	 * <b>
//	 * It is very important the the explicit write set contains <b>at least</b> all fields that may
//	 * have been modified in order to guarantee correct execution of PACT programs. 
//	 * So if in doubt, add a field to the modification annotation.
//	 * </b>
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ExplicitModifications
//	{
//		int[] fields();
//	}
//	
//	
//	/**
//	 * The OutputCardBounds annotation specifies lower and upper bounds on the number of 
//	 * records emitted by the stub.
//	 * A constants are provided for specical values such as unbounded, cardinality of input, and 
//	 * cardinality of first and second input.
//	 *
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface OutCardBounds
//	{
//		public static final int UNKNOWN = -1;
//		public static final int UNBOUNDED = -2;
//		public static final int INPUTCARD = -3;
//		public static final int FIRSTINPUTCARD = -4;
//		public static final int SECONDINPUTCARD = -5;
//		
//		int lowerBound();
//		int upperBound();
//	}
//	
	/**
	 * Private constructor to prevent instantiation. This class is intended only as a container.
	 */
	private StubAnnotation() {
	}
}