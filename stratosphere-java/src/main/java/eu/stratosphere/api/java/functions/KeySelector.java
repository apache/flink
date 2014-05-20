/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.java.functions;


/**
 * The {@link KeySelector} allows to use arbitrary objects for operations such as
 * reduce, reduceGroup, join, coGoup, etc.
 * 
 * The extractor takes an object and returns the key for that object.
 *
 * @param <IN> Type of objects to extract the key from.
 * @param <KEY> Type of key.
 */
public abstract class KeySelector<IN, KEY> implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * User-defined function that extracts the key from an arbitrary object.
	 * 
	 * For example for a class:
	 * <pre>
	 * 	public class Word {
	 * 		String word;
	 * 		int count;
	 * 	}
	 * </pre>
	 * The key extractor could return the word as
	 * a key to group all Word objects by the String they contain.
	 * 
	 * The code would look like this
	 * <pre>
	 * 	public String getKey(Word w) {
	 * 		return w.word;
	 * 	}
	 * </pre>
	 * 
	 * @param value The object to get the key from.
	 * @return The extracted key.
	 */
	public abstract KEY getKey(IN value);
}
