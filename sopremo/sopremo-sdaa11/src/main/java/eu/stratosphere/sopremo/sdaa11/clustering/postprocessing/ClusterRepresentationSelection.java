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
package eu.stratosphere.sopremo.sdaa11.clustering.postprocessing;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author skruse
 * 
 */
public class ClusterRepresentationSelection extends
		ElementaryOperator<ClusterRepresentationSelection> {

	/**
	 * Describes the flag which will be selection criterion of incoming
	 * cluster representations.
	 */
	private static final long serialVersionUID = 459057198976528982L;

	private int flag;

	/**
	 * Returns the flag.
	 * 
	 * @return the flag
	 */
	public int getFlag() {
		return this.flag;
	}

	/**
	 * Sets the flag to the specified value.
	 * 
	 * @param flag
	 *            the flag to set
	 */
	public void setFlag(final int flag) {
		this.flag = flag;
	}

	public static class Implementation extends SopremoMap {

		private int flag;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			final int flag = ((IntNode) ((ObjectNode) value).get("flag"))
					.getIntValue();
			if (this.flag == flag)
				out.collect(value);
		}
	}

}
