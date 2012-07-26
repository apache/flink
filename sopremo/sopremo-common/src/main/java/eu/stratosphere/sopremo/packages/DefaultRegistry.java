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

/**
 * Default implementation of {@link IRegistry}.
 * 
 * @author Arvid Heise
 */
public class DefaultRegistry<T> implements IRegistry<T> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8600814085154060117L;
	
	private Map<String, T> elements = new HashMap<String, T>();

	@Override
	public T get(String name) {
		return this.elements.get(name);
	}

	@Override
	public void put(String name, T element) {
		this.elements.put(name, element);
	}

	@Override
	public Set<String> keySet() {
		return Collections.unmodifiableSet(this.elements.keySet());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Registry").append(this.elements);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}
}
