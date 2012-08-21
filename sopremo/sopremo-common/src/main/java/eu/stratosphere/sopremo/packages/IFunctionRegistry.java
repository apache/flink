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
package eu.stratosphere.sopremo.packages;

import java.lang.reflect.Method;

import eu.stratosphere.sopremo.function.Callable;

/**
 * Registry that manages {@link SopremoFunction}s.
 * 
 * @author Arvid Heise
 */
public interface IFunctionRegistry extends IRegistry<Callable<?, ?>> {
	public void put(final Method method);

	public void put(final Class<?> javaFunctions);

	public void put(String registeredName, final Class<?> clazz, final String staticMethodName);
}