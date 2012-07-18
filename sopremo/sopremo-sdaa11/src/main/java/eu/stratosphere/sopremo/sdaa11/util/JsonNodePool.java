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
package eu.stratosphere.sopremo.sdaa11.util;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class JsonNodePool<Node extends IJsonNode> {

	private final IJsonNodeFactory<Node> factory;
	private final List<Node> nodes = new ArrayList<Node>();
	private int index = 0;

	/**
	 * Initializes JsonNodePool.
	 * 
	 * @param factory
	 */
	public JsonNodePool(final IJsonNodeFactory<Node> factory) {
		this.factory = factory;
	}

	public Node getNode() {
		while (this.index >= this.nodes.size())
			this.nodes.add(this.factory.createJsonNode());
		return this.nodes.get(this.index++);
	}

	public void reset() {
		this.index = 0;
	}

	public static interface IJsonNodeFactory<Node extends IJsonNode> {
		Node createJsonNode();
	}

	public static class TextNodeFactory implements IJsonNodeFactory<TextNode> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.sdaa11.util.JsonNodePool.IJsonNodeFactory
		 * #createJsonNode()
		 */
		@Override
		public TextNode createJsonNode() {
			return new TextNode();
		}
	}

}
