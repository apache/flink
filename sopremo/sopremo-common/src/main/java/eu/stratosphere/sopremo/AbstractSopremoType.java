/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo;

/**
 * Provides basic implementations of the required methods of {@link SopremoType}
 * 
 * @author Arvid Heise
 */
public abstract class AbstractSopremoType implements ISopremoType {
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toString(this);
	}

	/**
	 * Returns a string representation of the given {@link SopremoType}.
	 * 
	 * @param type
	 *        the SopremoType that should be used
	 * @return the string representation
	 */
	public static String toString(final ISopremoType type) {
		final StringBuilder builder = new StringBuilder();
		type.toString(builder);
		return builder.toString();
	}
}
