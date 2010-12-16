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

package eu.stratosphere.pact.common.contract;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This class defines the output contracts, realized as java annotations. To use an output contract, simply add the
 * annotation above the class declaration of the class that realized the user function. For example, to declare the
 * <i>SameKey</i> output contract for a map-type function, use it the following way:
 * 
 * <pre>
 * \@OutputContract.SameKey
 * public class MyMapper extends MapStub<N_Integer, N_String, N_Integer, N_String>
 * {
 *     protected void map(N_Integer key, N_String value, Collector<N_Integer, N_String> out)
 *     {
 *        String s = value.toString();
 *        
 *        if (s.contains("...some text...") {
 *            out.collect(key, value);
 *        }
 *     }
 * }
 * </pre>
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class OutputContract {
	/**
	 * Annotation representing the <b>Same-Key</b> output contract. A class implementing the user
	 * function of a PACT may declare this annotation. That way it assures to produce on each invocation
	 * only key/value pairs with the same key, as the key it was invoked with.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SameKey {
	};

	/**
	 * Annotation representing the <b>Super-Key</b> output contract. A class implementing the user
	 * function of a PACT may declare this annotation. That way it assures to produce on each invocation
	 * only key/value pairs, where the key is a super-key of the key it was invoked with.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SuperKey {
	};

	/**
	 * Annotation representing the <b>Unique-Key</b> output contract. A class implementing the user
	 * function of a PACT, or a data source may declare this annotation.
	 * That way the user function or data source assures to produce globally unique keys.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface UniqueKey {
	};

	/**
	 * Annotation representing the <b>Same-Key-Of-First-Input</b> output contract. A class implementing
	 * the user function of a PACT may declare this annotation, if it has two inputs with potentially
	 * different keys, such as for example a function using the <i>Cross</i> contract.
	 * <p>
	 * The meaning of this contract is the same as that of the Same-Key {@link #SameKey} contract, only referring to the
	 * <b>first</b> of the two keys in the current method invocation.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SameKeyFirst {
	};

	/**
	 * Annotation representing the <b>Super-Key-Of-First-Input</b> output contract. A class implementing
	 * the user function of a PACT may declare this annotation, if it has two inputs with potentially
	 * different keys, such as for example a function using the <i>Cross</i> contract.
	 * <p>
	 * The meaning of this contract is the same as that of the Super-key {@link #SuperKey} contract, only referring to
	 * the <b>first</b> of the two keys in the current method invocation.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SuperKeyFirst {
	};

	/**
	 * Annotation representing the <b>Same-Key-Of-Second-Input</b> output contract. A class implementing
	 * the user function of a PACT may declare this annotation, if it has two inputs with potentially
	 * different keys, such as for example a function using the <i>Cross</i> contract.
	 * <p>
	 * The meaning of this contract is the same as that of the Same-Key {@link #SameKey} contract, but only referring to
	 * the <b>second</b> of the two keys in the current method invocation.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SameKeySecond {
	};

	/**
	 * Annotation representing the <b>Super-Key-Of-Second-Input</b> output contract. A class implementing
	 * the user function of a PACT may declare this annotation, if it has two inputs with potentially
	 * different keys, such as for example a function using the <i>Cross</i> contract.
	 * <p>
	 * The meaning of this contract is the same as that of the Super-key {@link #SuperKey} contract, only referring to
	 * the <b>second</b> of the two keys in the current method invocation.
	 */
	@Target(ElementType.TYPE)
	@Retention(RetentionPolicy.RUNTIME)
	public @interface SuperKeySecond {
	};

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * Private constructor to prevent instantiation. This class is intended only as a container.
	 */
	private OutputContract() {
	}
}
