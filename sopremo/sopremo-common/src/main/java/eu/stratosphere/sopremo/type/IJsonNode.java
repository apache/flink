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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NormalizableKey;
import eu.stratosphere.pact.common.type.Value;

/**
 * Interface for all JsonNodes.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public interface IJsonNode extends Serializable, Value, NormalizableKey, Cloneable {

	public abstract void clear();
	
	/**
	 * Returns the {@link eu.stratosphere.sopremo.type.JsonNode.Type} of this node.
	 * 
	 * @return nodetype
	 */
	public abstract JsonNode.Type getType();

	/**
	 * Transforms this node into his standard representation.
	 * 
	 * @return standard representation
	 */
	public abstract IJsonNode canonicalize();

	/**
	 * Duplicates this node.
	 * 
	 * @return duplicate or null if Exception occurs
	 */
	public abstract IJsonNode clone();

	/**
	 * Deserializes this node from a DataInput.
	 * 
	 * @param {@link DataInput} in
	 * @exception {@link IOException}
	 */
	@Override
	public abstract void read(DataInput in) throws IOException;

	/**
	 * Serializes this node into a DataOutput.
	 * 
	 * @param {@link DataOutput} out
	 * @exception {@link IOException}
	 */
	@Override
	public abstract void write(DataOutput out) throws IOException;

	/**
	 * Returns either this node is a representation for a null-value or not.
	 */
	public abstract boolean isNull();

	/**
	 * Returns either this node is a representation for a missing value or not.
	 */
	public abstract boolean isMissing();

	/**
	 * Returns either this node is a representation of a Json-Object or not.
	 */
	public abstract boolean isObject();

	/**
	 * Returns either this node is a representation of a Json-Array or not.
	 */
	public abstract boolean isArray();

	/**
	 * Returns either this node is a representation of a Text-value or not.
	 */
	public abstract boolean isTextual();

	/**
	 * Returns the internal representation of this nodes value.
	 * 
	 * @return this nodes value
	 */
	public abstract Object getJavaValue();

	/**
	 * Compares this node with another.
	 * 
	 * @param other
	 *        the node this node should be compared with
	 * @return result of the comparison
	 */
	@Override
	public abstract int compareTo(final Key other);

	/**
	 * Compares this node with another {@link eu.stratosphere.sopremo.type.IJsonNode}.
	 * 
	 * @param other
	 *        the node this node should be compared with
	 * @return result of the comparison
	 */
	public abstract int compareToSameType(IJsonNode other);

	/**
	 * Appends this nodes string representation to a given {@link StringBuilder}.
	 * 
	 * @param sb
	 *        the StringBuilder
	 * @return a StringBuilder where this nodes string representation is appended
	 */
	public abstract StringBuilder toString(StringBuilder sb);

}