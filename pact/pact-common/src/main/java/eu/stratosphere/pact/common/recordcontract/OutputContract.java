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

package eu.stratosphere.pact.common.recordcontract;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import eu.stratosphere.pact.common.type.PactRecord;


/**
 * This class defines the output contracts, realized as java annotations. To use
 * an output contract, simply add the annotation above the class declaration of
 * the class that realized the user function. For example, to declare the
 * <i>AllSame</i> output contract for a map-type function that realizes a simple filter,
 * use it the following way:
 * 
 * <pre><blockquote>
 * \@OutputContract.AllConstant
 * public class MyMapper extends MapStub
 * {
 *     public void map(PactRecord record, Collector out)
 *     {
 *        String s = record.getField(2, PactString.class).toString();
 *        
 *        if (s.contains("...some text...") {
 *            out.collect(record);
 *        }
 *     }
 * }
 * </blockquote></pre>
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public enum OutputContract
{
	AllConstant,
	Constant,
	Unique,
	SemanticalDependency;
	
	/**
	 * Annotation representing the <i>AllConstant</i> output contract. That output contract indicates that all fields are
	 * unchanged. It effectively signifies that, if a record is produced, it is an exact copy of the record(s) the
	 * stub was invoked with.
	 * <p>
	 * An exception applies to fields that are null in the input. Those fields may be set by the function, still being
	 * consistent with the definition of this output contract.
	 * <p>
	 * This contract can be applied to contracts with more than one input. The parameter <tt>input</tt> defines which input
	 * the contract refers to. If the parameter is set to <i>-1</i>, it refers to both inputs. If this output contract is
	 * applied to both inputs of a user function which has multiple inputs (such as for a
	 * <i>Match</i> or <i>Cross</i> Pact), it indicates that all fields from both inputs are constant. That is possible,
	 * if for both inputs, their non-null fields are null in the respective other input. (See also 
	 * {@link PactRecord#unionFields(PactRecord)} for an example.)
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface AllConstant
	{
		/**
		 * Defines the input that this contract refers to. The first input is labeled zero, the second one,
		 * and so forth.
		 * <p>
		 * If not specified explicitly, this parameter is <i>-1</i>, defining that the contract applies to both inputs.
		 * 
		 * @return The number of the input, starting at zero.  
		 */
		int input() default -1;
	};
//	
//	/**
//	 * Annotation representing the <i>AllConstantFirst</i> output contract. That output contract indicates that the output
//	 * records retain all fields from the FIRST input unchanged and at the same position.
//	 * <p>
//	 * An exception applies to fields that are null in the input. Those fields may be set by the function, still being
//	 * consistent with the definition of this output contract.
//	 * <p>
//	 * This output contract is only applicable to stubs with two inputs.
//	 * 
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface AllConstantFirst {};
//	
//	/**
//	 * Annotation representing the <i>AllConstantSecond</i> output contract. That output contract indicates that the output
//	 * records retain all fields from the SECOND input unchanged and at the same position.
//	 * <p>
//	 * An exception applies to fields that are null in the input. Those fields may be set by the function, still being
//	 * consistent with the definition of this output contract.
//	 * <p>
//	 * This output contract is only applicable to stubs with two inputs.
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface AllConstantSecond {};
	
	/**
	 * Annotation representing the <i>ConstantFields</i> output contract. That output contract indicates that certain fields are
	 * unchanged. When the user function is invoked with a record, the records emitted from this call must have those fields
	 * unchanged at the same position.
	 * <p>
	 * An exception applies to fields that are null in the input. Those fields may be set by the function, still being
	 * consistent with the definition of this output contract.
	 * <p>
	 * This contract can be applied to contracts with more than one input. The parameter <tt>input</tt> defines which input
	 * the contract refers to.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Constant {
		/**
		 * Parameter used to specify which field(s) of the record that remain unchanged.
		 * 
		 * @return The field(s) that is (are) unchanged.
		 */
		int[] value();
		
		/**
		 * Defines the input that this contract refers to. The first input is labeled zero, the second one,
		 * and so forth.
		 * 
		 * @return The number of the input, starting at zero.  
		 */
		int input() default 0;
	};
	
//	/**
//	 * Annotation representing the <i>ConstantFields</i> output contract referring to the FIRST input.
//	 * The contract indicates that certain fields in the output are unchanged with respect to the first input record.
//	 * When the user function is invoked with a record, the records emitted from this call must have at those positions
//	 * the same fields unchanged at the same position as the record from the first input.
//	 * <p>
//	 * An exception applies to fields that are null in the input. Those fields may be set by the function, still being
//	 * consistent with the definition of this output contract.
//	 * <p>
//	 * This output contract is only applicable to stubs with two inputs.
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ConstantFirst {
//		/**
//		 * Parameter used to specify which field(s) of the first input's record remain unchanged.
//		 * 
//		 * @return The field(s) that is (are) unchanged.
//		 */
//		int[] value();
//	};
//	
//	/**
//	 * Annotation representing the <i>ConstantFields</i> output contract referring to the SECOND input.
//	 * The contract indicates that certain fields in the output are unchanged with respect to the second input record.
//	 * When the user function is invoked with a record, the records emitted from this call must have at those positions
//	 * the same fields unchanged at the same position as the record from the second input.
//	 * <p>
//	 * An exception applies to fields that are null in the input. Those fields may be set by the function, still being
//	 * consistent with the definition of this output contract.
//	 * <p>
//	 * This output contract is only applicable to stubs with two inputs.
//	 */
//	@Target(ElementType.TYPE)
//	@Retention(RetentionPolicy.RUNTIME)
//	public @interface ConstantSecond {
//		/**
//		 * Parameter used to specify which field(s) of the first input's record remain unchanged.
//		 * 
//		 * @return The field(s) that is (are) unchanged.
//		 */
//		int[] value();
//	};
	
	/**
	 * Annotation representing the <i>UniqueFields</i> output contract. It signifies that a combination of fields
	 * has a globally unique value across all records.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Unique
	{
		/**
		 * Parameter used to specify which field(s) of the record form a unique value.
		 * 
		 * @return The field(s) that is (are) unique in the produced tuple.
		 */
		int[] value();
	}
	
	/**
	 * Annotation representing the <i>SuperKey</i> output contract. It signifies that a field in the output 
	 * is a super-key with respect to the field at that position in the input. For example, if a field in the
	 * input contains the value <i>x</i> and the field in the output at that position contains the value
	 * <i>(x, y)</i>, than that field is a super-key with respect to the input field. 
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SuperKey
	{
		/**
		 * Parameter used to specify which field of the output record is a super-key of its corresponding input field. 
		 * 
		 * @return The position of the field.
		 */
		int value();
		
		/**
		 * Defines the input that this contract refers to. The first input is labeled zero, the second one,
		 * and so forth.
		 * 
		 * @return The number of the input, starting at zero.  
		 */
		int input() default 0;
	}

	/**
	 * Annotation representing a semantical dependency 
	 * 
	 * <p>
	 * This contract can be applied to contracts with more than one input. The parameter <tt>input</tt> defines which input
	 * the contract refers to.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SameField
	{
		int outputField();
		
		int inputField();
		
		/**
		 * Defines the input that this contract refers to. The first input is labeled zero, the second one,
		 * and so forth.
		 * 
		 * @return The number of the input, starting at zero.  
		 */
		int input() default 0;
	}
	
	/**
	 * Annotation representing a semantical dependency 
	 * 
	 * <p>
	 * This contract can be applied to contracts with more than one input. The parameter <tt>input</tt> defines which input
	 * the contract refers to.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface DerivedField
	{
		int targetField();
		
		int[] sourceFields();
		
		/**
		 * Defines the input that this contract refers to. The first input is labeled zero, the second one,
		 * and so forth.
		 * 
		 * @return The number of the input, starting at zero.  
		 */
		int[] input() default 0;
	}
	
	// --------------------------------------------------------------------------------------------
	
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Private constructor to prevent instantiation. This class is intended only as a container.
	 */
	private OutputContract() {
	}
}
