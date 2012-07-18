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
package eu.stratosphere.sopremo.sdaa11;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Prints a JSON stream and forwards it.
 * 
 * @author skruse
 * 
 */
@InputCardinality(value = 1)
@OutputCardinality(value = 1)
public class PrintAndForward extends ElementaryOperator<PrintAndForward> {

	private static final long serialVersionUID = 5955630169271456512L;

	private String tag = "[some_tag]";

	/**
	 * Returns the tag.
	 * 
	 * @return the tag
	 */
	public String getTag() {
		return this.tag;
	}

	/**
	 * Sets the tag to the specified value.
	 * 
	 * @param tag
	 *            the tag to set
	 */
	public void setTag(final String tag) {
		if (tag == null)
			throw new NullPointerException("tag must not be null");

		this.tag = tag;
	}

	public static class Implementation extends SopremoMap {

		String tag;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			System.out.print(this.tag);
			System.out.print(' ');
			System.out.println(value);
			out.collect(value);
		}

	}

}
