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
package eu.stratosphere.pact.testing;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * @author Arvid Heise
 */
public abstract class AbstractValueSimilarity<V extends Value> implements ValueSimilarity<V> {
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.testing.ValueSimilarity#isApplicable(java.lang.Class)
	 */
	@Override
	public boolean isApplicable(Class<? extends V> valueType) {
		return ReflectionUtil.getTemplateType1(this.getClass()).isAssignableFrom(valueType);
	}
}
