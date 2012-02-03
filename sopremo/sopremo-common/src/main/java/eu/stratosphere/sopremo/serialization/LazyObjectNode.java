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
package eu.stratosphere.sopremo.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.JsonNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 *
 */
public class LazyObjectNode extends JsonNode implements IObjectNode{

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#getType()
	 */
	@Override
	public Type getType() {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		// TODO auto generated method stub
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO auto generated method stub
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#getJavaValue()
	 */
	@Override
	public Object getJavaValue() {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#compareToSameType(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public int compareToSameType(IJsonNode other) {
		// TODO auto generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#toString(java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder toString(StringBuilder sb) {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#put(java.lang.String, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IObjectNode put(String fieldName, IJsonNode value) {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#get(java.lang.String)
	 */
	@Override
	public IJsonNode get(String fieldName) {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#remove(java.lang.String)
	 */
	@Override
	public IJsonNode remove(String fieldName) {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#removeAll()
	 */
	@Override
	public IObjectNode removeAll() {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#getEntries()
	 */
	@Override
	public Set<Entry<String, IJsonNode>> getEntries() {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#putAll(eu.stratosphere.sopremo.type.JsonObject)
	 */
	@Override
	public IObjectNode putAll(IObjectNode jsonNode) {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#getFieldNames()
	 */
	@Override
	public Iterator<String> getFieldNames() {
		// TODO auto generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.JsonObject#getFields()
	 */
	@Override
	public Iterator<Entry<String, IJsonNode>> getFields() {
		// TODO auto generated method stub
		return null;
	}

}
