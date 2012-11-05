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
package eu.stratosphere.sopremo.tokenizer;

import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public interface Tokenizer extends ISerializableSopremoType {

	/**
	 * Tokenizes the given text into the specified array node. The tokenizer should try to reuse existing nodes in the
	 * {@link CachingArrayNode} of {@link eu.stratosphere.sopremo.type.TextNode}s.
	 * 
	 * @param text
	 *        the text to tokenize
	 * @param tokens
	 *        the target array of {@link eu.stratosphere.sopremo.type.TextNode}s.
	 */
	void tokenizeInto(CharSequence text, CachingArrayNode tokens);

}
