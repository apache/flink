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

import eu.stratosphere.pact.common.stubs.StubAnnotation.ImplicitOperation.ImplicitOperationMode;
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

	/**
	 * Specifies the read set for a stub with a single input ( {@link MapStub}, {@link ReduceStub}).
	 * The {@link Reads#fields()} attribute of the read set specifies all fields of the record 
	 * that the stub reads and uses to modify its output. 
	 * This is the case for if the value is used in conditional statements or to compute new values.
	 * 
	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
	 * correct execution of PACT programs. 
	 * 
	 * If no ReadSet is specified, all fields are considered to be read.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Reads
	{
		int[] fields();
	}
	
	/**
	 * Specifies the read set for the first input of a stub with two inputs ( {@link CrossStub}, 
	 * {@link MatchStub}, {@link CoGroupStub}).
	 * The {@link ReadsFirst#fields()} attribute of the read set specifies all fields of the record 
	 * that the stub reads from the first input's tuples and uses to modify its output. 
	 * This is the case for if the value is used in conditional statements or to compute new values.   
	 *
	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
	 * correct execution of PACT programs. 
	 *
	 * If no ReadSet is specified, all fields are considered to be read.
	 * 
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadsFirst
	{
		int[] fields();
	}
	
	/**
	 * Specifies the read set for the second input of a stub with two inputs ( {@link CrossStub}, 
	 * {@link MatchStub}, {@link CoGroupStub}).
	 * The {@link ReadsSecond#fields()} attribute of the read set specifies all fields of the record 
	 * that the stub reads from the second input's tuples and uses to modify its output. 
	 * This is the case for if the value is used in conditional statements or to compute new values.   
	 *
	 * It is very important the the ReadSet contains at least all fields that are read in order to guarantee 
	 * correct execution of PACT programs. 
	 * 
	 * If no ReadSet is specified, all fields are considered to be read.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ReadsSecond
	{
		int[] fields();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ImplicitOperation
	{
		public static enum ImplicitOperationMode {Copy, Projection};
		
		ImplicitOperationMode implicitOperation();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ImplicitOperationFirst
	{
		ImplicitOperationMode implicitOperation();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ImplicitOperationSecond
	{
		ImplicitOperationMode implicitOperation();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ExplicitCopies
	{
		int[] fields();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ExplicitCopiesFirst
	{
		int[] fields();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ExplicitCopiesSecond
	{
		int[] fields();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ExplicitProjections
	{
		int[] fields();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ExplicitProjectionsFirst
	{
		int[] fields();
	}
	
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ExplicitProjectionsSecond
	{
		int[] fields();
	}

	/**
	 * Specifies the explicit write set.
	 * The {@link ExplicitWrites#fields()} attribute contains all fields of the output record 
	 * to which the stub explicitly applies non-null modifications. 
	 * 
	 * An explicit modification is done by calling the {@link PactRecord#setField(int, eu.stratosphere.pact.common.type.Value)}
	 * method. 
	 * 
	 * It is very important the the explicit write set  contains <b>at least</b> all fields that 
	 * have been modified in order to guarantee correct execution of PACT programs. 
	 * Specifying a superset is not critical.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface ExplicitWrites
	{
		int[] fields();
	}
	
	
	/**
	 * The OutputCardBounds annotation specifies lower and upper bounds on the number of 
	 * records emitted by the stub.
	 * A constants are provided for specical values such as unbounded, cardinality of input, and 
	 * cardinality of first and second input.
	 *
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface OutCardBounds
	{
		public static final int UNKNOWN = -1;
		public static final int UNBOUNDED = -2;
		public static final int INPUTCARD = -3;
		public static final int FIRSTINPUTCARD = -4;
		public static final int SECONDINPUTCARD = -5;
		
		int lowerBound();
		int upperBound();
	}
	
	/**
	 * Private constructor to prevent instantiation. This class is intended only as a container.
	 */
	private StubAnnotation() {
	}
}