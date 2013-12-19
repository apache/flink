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

package eu.stratosphere.api.common.operators.util;

import java.lang.annotation.Annotation;

/**
 * Interface defining that an output contract can be attached to an object.
 */
public interface AnnotationConfigurable
{
	/**
	 * Adds an output contract.
	 * 
	 * @param clazz The class of the OutputContract to attach.
	 */
	public void addStubAnnotation(Class<? extends Annotation> clazz);

	/**
	 * Returns the output contracts that were attached to the object.
	 * 
	 * @return An array containing the classes of the attached output contracts.
	 */
	public Class<? extends Annotation>[] getStubAnnotation();
}
