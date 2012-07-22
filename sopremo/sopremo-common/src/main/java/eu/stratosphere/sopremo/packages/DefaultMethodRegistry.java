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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.sopremo.function.SopremoMethod;

/**
 * @author Arvid Heise
 */
public class DefaultMethodRegistry extends AbstractMethodRegistry {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7660926261580208403L;
	private Map<String, SopremoMethod> methods = new HashMap<String, SopremoMethod>();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.AbstractMethodRegistry#findMethod(java.lang.String)
	 */
	@Override
	public SopremoMethod get(String name) {
		return this.methods.get(name);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.AbstractMethodRegistry#registerMethod(java.lang.String,
	 * eu.stratosphere.sopremo.function.MeteorMethod)
	 */
	@Override
	public void put(String name, SopremoMethod method) {
		this.methods.put(name, method);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.packages.IMethodRegistry#getRegisteredMethods()
	 */
	@Override
	public Set<String> keySet() {
		return Collections.unmodifiableSet(this.methods.keySet());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Method registry: ").append(this.methods);
	}
}
