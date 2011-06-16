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


/**
 * This class defines the output contracts, realized as java annotations. To use
 * an output contract, simply add the annotation above the class declaration of
 * the class that realized the user function. For example, to declare the
 * <i>AllSame</i> output contract for a map-type function that realizes a simple filter,
 * use it the following way:
 * 
 * <pre><blockquote>
 * \@OutputContract.AllSame
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
public class OutputContract
{
	/**
	 * Annotation representing the <i>AllSame</i> output contract. That output contract indicates that all fields are
	 * unchanged. It effectively signifies that, if a record is produced, it is an exact copy of the record(s) the
	 * stub was invoked with.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface AllSame {
	};
	
	/**
	 * Annotation representing the <i>AllSameFirst</i> output contract. That output contract indicates that the output
	 * records retain all fields from 
	 * unchanged.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface AllSame {
	};
	
	/**
	 * Annotation representing the <i>AllSame</i> output contract. That output contract indicates that all fields are
	 * unchanged.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface AllSame {
	};
	
	/**
	 * Annotation representing the <i>SameFields</i> output contract. That output contract indicates that certain fields are
	 * unchanged.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Same {
		/**
		 * Parameter used to specify which field(s) of the record remain unchanged.
		 * 
		 * @return The field(s) that is (are) unchanged.
		 */
		int[] value();
	};
	
	/**
	 * Annotation representing the <i>SameFields</i> output contract referring to the first input.
	 * The contract indicates that certain fields in the output are unchanged with respect to the first input record.
	 * <p>
	 * This output contract is only applicable to stubs with two inputs.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SameFirst {
		/**
		 * Parameter used to specify which field(s) of the first input's record remain unchanged.
		 * 
		 * @return The field(s) that is (are) unchanged.
		 */
		int[] value();
	};
	
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
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Private constructor to prevent instantiation. This class is intended only
	 * as a container.
	 */
	private OutputContract() {
	}
}
