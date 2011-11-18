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

package eu.stratosphere.nephele.plugins;

import eu.stratosphere.nephele.io.AbstractID;

/**
 * This class provides IDs to uniquely identify Nephele plugins.
 * 
 * @author warneke
 */
public final class PluginID extends AbstractID {

	private PluginID(final byte[] byteArray) {
		super(byteArray);
	}

	public static PluginID fromByteArray(final byte[] byteArray) {

		if (byteArray == null) {
			throw new IllegalArgumentException("Argument byteArray must not be null");
		}

		if (byteArray.length != SIZE) {
			throw new IllegalArgumentException("Provided byte array must have a length of " + SIZE);
		}

		return new PluginID(byteArray);
	}
}
