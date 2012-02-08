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
package eu.stratosphere.sopremo.type;

import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 *
 */
public interface IObjectNode extends IJsonNode, Iterable<Entry<String, IJsonNode>>{

	public abstract IObjectNode put(final String fieldName, final IJsonNode value);

	public abstract IJsonNode get(final String fieldName);

	public abstract IJsonNode remove(final String fieldName);

	public abstract IObjectNode removeAll();

	public abstract Set<Entry<String, IJsonNode>> getEntries();

	public abstract IObjectNode putAll(final IObjectNode jsonNode);

	public abstract Iterator<String> getFieldNames();

	public abstract Iterator<Entry<String, IJsonNode>> iterator();
	
	public abstract int size();

}