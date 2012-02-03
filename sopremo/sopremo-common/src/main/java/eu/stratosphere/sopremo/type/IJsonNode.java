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
import eu.stratosphere.pact.common.type.Value;

/**
 * @author strato
 *
 */
public interface IJsonNode extends Serializable, Value, Key, Cloneable{

	public abstract JsonNode.Type getType();

	public abstract IJsonNode canonicalize();

	public abstract IJsonNode clone();

	public abstract void read(DataInput in) throws IOException;

	public abstract void write(DataOutput out) throws IOException;

	public abstract boolean isNull();

	public abstract boolean isObject();

	public abstract boolean isArray();

	public abstract boolean isTextual();

	public abstract Object getJavaValue();

	public abstract int compareTo(final Key other);

	public abstract int compareToSameType(IJsonNode other);

	public abstract StringBuilder toString(StringBuilder sb);

}