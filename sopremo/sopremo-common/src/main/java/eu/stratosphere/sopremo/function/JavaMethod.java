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
package eu.stratosphere.sopremo.function;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.reflect.DynamicMethod;
import eu.stratosphere.util.reflect.Signature;

/**
 * @author Arvid Heise
 */
public abstract class JavaMethod extends SopremoFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2195013413330805401L;

	protected final DynamicMethod<IJsonNode> method;

	private transient List<Object[]> paramCache = new ArrayList<Object[]>();

	/**
	 * Initializes JavaMethod.
	 */
	public JavaMethod(final String name) {
		this.method = new DynamicMethod<IJsonNode>(name);
	}

	protected Object[] addTargetToParameters(final IArrayNode params, final IJsonNode target) {
		final int numParams = params.size();
		for (int cacheSize = this.paramCache.size(); cacheSize <= numParams; cacheSize++)
			this.paramCache.add(new Object[cacheSize + 1]);
		final Object[] expandedParams = this.paramCache.get(numParams);
		expandedParams[0] = target;
		for (int index = 0; index < numParams; index++)
			expandedParams[index + 1] = params.get(index);
		return expandedParams;
	}

	public void addSignature(final Method method) {
		this.method.addSignature(method);
	}

	public Collection<Signature> getSignatures() {
		return this.method.getSignatures();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.method.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JavaMethod other = (JavaMethod) obj;
		return this.method.equals(other.method);
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append("Java method ").append(this.method);
	}

}