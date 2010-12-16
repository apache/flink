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

package eu.stratosphere.pact.compiler;

import java.lang.annotation.Annotation;

/**
 * The compiler representation of an output contract. Used for convenience to avoid messing with
 * reflection objects in the compiler.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public enum OutputContract {
	None(null, 0), SameKey(eu.stratosphere.pact.common.contract.OutputContract.SameKey.class, 0), SuperKey(
			eu.stratosphere.pact.common.contract.OutputContract.SuperKey.class, 0), UniqueKey(
			eu.stratosphere.pact.common.contract.OutputContract.UniqueKey.class, 0), SameKeyFirst(
			eu.stratosphere.pact.common.contract.OutputContract.SameKeyFirst.class, 1), SameKeySecond(
			eu.stratosphere.pact.common.contract.OutputContract.SameKeySecond.class, 2), SuperKeyFirst(
			eu.stratosphere.pact.common.contract.OutputContract.SuperKeyFirst.class, 1), SuperKeySecond(
			eu.stratosphere.pact.common.contract.OutputContract.SuperKeySecond.class, 2);

	// ------------------------------------------------------------------------

	/**
	 * Gets the OutputContract enum constant that corresponds to the output contract given through the annotation.
	 * The annotation is represented through its class.
	 * 
	 * @param contractAnnotationClass
	 *        The output contract annotation, represented through its class.
	 * @return The corresponding output contract enum constant.
	 */
	public static OutputContract getOutputContract(Class<? extends Annotation> contractAnnotationClass) {
		OutputContract[] values = values();
		for (int i = 0; i < values.length; i++) {
			if (values[i].annotationClass == contractAnnotationClass) {
				return values[i];
			}
		}

		throw new IllegalArgumentException("The given class does not describe an output contract.");
	}

	// ------------------------------------------------------------------------

	public boolean isSideSpecific() {
		return side != NOT_SIDE_SPECIFIC;
	}

	public boolean appliesToFirstInput() {
		return side == SIDE_FIRST;
	}

	public boolean appliesToSecondInput() {
		return side == SIDE_SECOND;
	}

	// ------------------------------------------------------------------------
	// Privates
	// ------------------------------------------------------------------------

	/**
	 * Creates a new constant that represents the output contract reflected by the annotation of the
	 * given class.
	 * 
	 * @param clazz
	 *        The class of the output contract annotation that corresponds to
	 *        this enum constant.
	 */
	private OutputContract(Class<? extends Annotation> clazz, int side) {
		this.annotationClass = clazz;
		this.side = side;
	}

	private Class<? extends Annotation> annotationClass; // the class of the output contract annotation

	// that corresponds to this enum constant

	private int side; // the input side that the contract applies to

	private static final int NOT_SIDE_SPECIFIC = 0; // for contracts with one key input only

	private static final int SIDE_FIRST = 1; // for contracts with two keys input applied to the first input

	private static final int SIDE_SECOND = 2; // for contracts with two keys input applied to the second input
}
