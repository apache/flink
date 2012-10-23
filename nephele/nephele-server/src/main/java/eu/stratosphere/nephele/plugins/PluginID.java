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

package eu.stratosphere.nephele.plugins;

import eu.stratosphere.nephele.io.AbstractID;

/**
 * A class for statistically unique Nephele plugin IDs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class PluginID extends AbstractID {

	/**
	 * Default constructor required by kryo.
	 */
	private PluginID() {
	}

	/**
	 * Constructs a new plugin ID.
	 * 
	 * @param lowerPart
	 *        the lower bytes of the ID
	 * @param upperPart
	 *        the higher bytes of the ID
	 */
	private PluginID(final long lowerPart, final long upperPart) {
		super(lowerPart, upperPart);
	}
}
