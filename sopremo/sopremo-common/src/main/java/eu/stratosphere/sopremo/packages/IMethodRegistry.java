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
import java.util.Map;

import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.function.SopremoMethod;

/**
 * Registry that manages {@link MeteorMethod}s.
 * 
 * @author Arvid Heise
 */
public interface IMethodRegistry extends ISerializableSopremoType {
	public abstract SopremoMethod findMethod(String name);

	public abstract void registerMethod(String name, SopremoMethod method);

	public abstract void register(final Method method);

	public abstract void register(final Class<?> javaFunctions);

	public Map<String, SopremoMethod> getRegisteredMethods();
}