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

package eu.stratosphere.pact.common.stubs;

import eu.stratosphere.pact.common.util.ReflectionUtil;

import eu.stratosphere.pact.common.type.Key;

/**
 * Abstract stub class for all PACT stubs with a single input.
 * PACT stubs must be overwritten to provide user implementations for PACT programs.
 * 
 * @author Fabian Hueske
 * @param <K> Type of the input key.
 */
abstract class SingleInputKeyStub<K extends Key> extends Stub
{
	/**
	 * Input key type.
	 */
	protected Class<K> keyClass;

	/**
	 * {@inheritDoc}
	 */
	protected void initTypes() {
		this.keyClass = ReflectionUtil.getTemplateType(getClass(), 0);
	}

	/**
	 * Returns the type of the input key.
	 * 
	 * @return Type of the input key.
	 */
	public Class<K> getKeyType() {
		return this.keyClass;
	}
}
