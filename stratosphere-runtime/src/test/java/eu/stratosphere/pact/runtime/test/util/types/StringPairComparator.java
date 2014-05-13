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

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.StringValue;

public class StringPairComparator extends TypeComparator<StringPair> {
	
	private static final long serialVersionUID = 1L;
	
	private String reference;

	@Override
	public int hash(StringPair record) {
		return record.getKey().hashCode();
	}

	@Override
	public void setReference(StringPair toCompare) {
		this.reference = toCompare.getKey();
	}

	@Override
	public boolean equalToReference(StringPair candidate) {
		return this.reference.equals(candidate.getKey());
	}

	@Override
	public int compareToReference(TypeComparator<StringPair> referencedComparator) {
		return this.reference.compareTo(((StringPairComparator)referencedComparator).reference);
	}
	
	@Override
	public int compare(StringPair first, StringPair second) {
		return first.getKey().compareTo(second.getKey());
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource)
			throws IOException {
		return StringValue.readString(firstSource).compareTo(StringValue.readString(secondSource));
	}

	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return false;
	}

	@Override
	public void putNormalizedKey(StringPair record, MemorySegment target,
			int offset, int numBytes) {
		throw new RuntimeException("not implemented");		
	}

	@Override
	public void writeWithKeyNormalization(StringPair record,
			DataOutputView target) throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public StringPair readWithKeyDenormalization(StringPair record,
			DataInputView source) throws IOException {
		throw new RuntimeException("not implemented");		
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public TypeComparator<StringPair> duplicate() {
		return new StringPairComparator();
	}
}
