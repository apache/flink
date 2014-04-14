/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.runtime.test.util.types;

import eu.stratosphere.api.common.typeutils.TypePairComparator;

public class StringPairPairComparator extends
		TypePairComparator<StringPair, StringPair> {
	
	private String reference;

	@Override
	public void setReference(StringPair reference) {
		this.reference = reference.getKey();
	}

	@Override
	public boolean equalToReference(StringPair candidate) {
		return reference.equals(candidate.getKey());
	}

	@Override
	public int compareToReference(StringPair candidate) {
		return reference.compareTo(candidate.getKey());
	}

}
