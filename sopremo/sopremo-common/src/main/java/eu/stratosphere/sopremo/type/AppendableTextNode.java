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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode.Type;

/**
 * Internal type that is used in string intensive aggregation operations.
 * 
 * @author Arvid Heise
 */

public class AppendableTextNode extends AbstractJsonNode implements Appendable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4663376747000392562L;

	public final static TextNode EMPTY_STRING_NODE = new TextNode("");

	private transient StringBuilder value;

	/**
	 * Initializes a TextNode which represents an empty String.
	 */
	public AppendableTextNode() {
		this.value = new StringBuilder();
	}

	@Override
	public StringBuilder append(CharSequence s) {
		return this.value.append(s);
	}

	@Override
	public StringBuilder append(CharSequence s, int start, int end) {
		return this.value.append(s, start, end);
	}

	@Override
	public StringBuilder append(char c) {
		return this.value.append(c);
	}

	/**
	 * Initializes a TextNode which represents the given <code>String</code>. To create new TextNodes please
	 * use TextNode.valueOf(<code>String</code>) instead.
	 * 
	 * @param v
	 *        the value that should be represented by this node
	 */
	public AppendableTextNode(final String v) {
		this.value = new StringBuilder(v);
	}

	@Override
	public StringBuilder getJavaValue() {
		return this.value;
	}

	/**
	 * Creates a new instance of TextNode. This new instance represents the given value.
	 * 
	 * @param v
	 *        the value that should be represented by the new instance
	 * @return the newly created instance of TextNode
	 */
	public static TextNode valueOf(final String v) {
		if (v == null)
			throw new NullPointerException();
		if (v.length() == 0)
			return EMPTY_STRING_NODE;
		return new TextNode(v);
	}

	/**
	 * Returns the String which is represented by this node.
	 * 
	 * @return the represented String
	 */
	public String getTextValue() {
		return this.getJavaValue().toString();
	}

	public void setValue(String value) {
		this.value.setLength(0);
		this.value.append(value);
	}

	@Override
	public StringBuilder toString(final StringBuilder sb) {
		appendQuoted(sb, this.value.toString());
		return sb;
	}

	/**
	 * Appends the given String with a leading and ending " to the given StringBuilder.
	 * 
	 * @param sb
	 *        the StringBuilder where the quoted String should be added to
	 * @param content
	 *        the String that should be appended
	 */
	public static void appendQuoted(final StringBuilder sb, final String content) {
		sb.append('"');
		sb.append(content);
		sb.append('"');
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.value.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;

		final AppendableTextNode other = (AppendableTextNode) obj;
		if (!this.value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		throw new IllegalStateException("Should never be used in Pact");
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		throw new IllegalStateException("Should never be used in Pact");
	}

	@Override
	public boolean isTextual() {
		return true;
	}

	@Override
	public Type getType() {
		return Type.TextNode;
	}

	@Override
	public int compareToSameType(final IJsonNode other) {
		return this.getTextValue().compareTo(((AppendableTextNode) other).getTextValue());
	}

	private void writeObject(final ObjectOutputStream out) throws IOException {
		out.writeUTF(getTextValue());
	}

	private void readObject(final ObjectInputStream in) throws IOException {
		this.value = new StringBuilder(in.readUTF());
	}

	@Override
	public void copyValueFrom(IJsonNode otherNode) {
		this.checkForSameType(otherNode);
		setValue(((AppendableTextNode) otherNode).getTextValue());
	}

	@Override
	public void clear() {
		setValue("");
	}
}
