/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ${packageName};
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.api.java.typeutils.runtime.TupleComparatorBase;
import org.apache.flink.api.java.typeutils.runtime.GenTypeComparatorProxy;
import org.apache.flink.util.InstantiationUtil;

public final class ${className} extends CompositeTypeComparator implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private final int[] normalizedKeyLengths;
	private final int numLeadingNormalizableKeys;
	private final int normalizableKeyPrefixLen;
	private final int numKeyFields;
	private final boolean invertNormKey;
	private ${serializerClassName} serializer;
	private final Class type;
	<#list members as m>
	${m}
	</#list>

	public ${className}(TypeComparator[] comparators, TypeSerializer serializer, Class type) {
		<#list initMembers as m>
		${m}
		</#list>
		this.type = type;
		this.numKeyFields = comparators.length;
		this.serializer = (${serializerClassName})serializer;
		this.normalizedKeyLengths = new int[numKeyFields];
		int nKeys = 0;
		int nKeyLen = 0;
		boolean inverted = f0.invertNormalizedKey();
		do {
			<#list normalizableKeys as n>
			${n}
			</#list>
		} while (false);
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
		this.invertNormKey = inverted;
	}

	private ${className}(${className} toClone) {
		<#list cloneMembers as m>
		${m}
		</#list>
		this.normalizedKeyLengths = toClone.normalizedKeyLengths;
		this.numKeyFields = toClone.numKeyFields;
		this.numLeadingNormalizableKeys = toClone.numLeadingNormalizableKeys;
		this.normalizableKeyPrefixLen = toClone.normalizableKeyPrefixLen;
		this.invertNormKey = toClone.invertNormKey;
		this.type = toClone.type;
		try {
			this.serializer = (${serializerClassName}) InstantiationUtil.deserializeObject(
				InstantiationUtil.serializeObject(toClone.serializer), Thread.currentThread().getContextClassLoader());
		} catch (IOException e) {
			throw new RuntimeException("Cannot copy serializer", e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Cannot copy serializer", e);
		}
	}

	@Override
	public void getFlatComparator(List flatComparators) {
		<#list flatComparators as fc>
		${fc}
		</#list>
	}

	@Override
	public int hash(Object value) {
		int i = 0;
		int code = 0;
		<#list hashMembers as hm>
		${hm}
		</#list>
		return code;
	}

	@Override
	public void setReference(Object toCompare) {
		<#list setReference as sr>
		${sr}
		</#list>
	}

	@Override
	public boolean equalToReference(Object candidate) {
		<#list equalToReference as er>
		${er}
		</#list>
		return true;
	}

	@Override
	public int compareToReference(TypeComparator referencedComparator) {
		${className} other = null;
		if (referencedComparator instanceof GenTypeComparatorProxy) {
			other = (${className})((GenTypeComparatorProxy)referencedComparator).getImpl();
		} else{
			other = (${className})referencedComparator;
		}
		int cmp;
		<#list compareToReference as cr>
		${cr}
		</#list>
		return 0;
	}

	@Override
	public int compare(Object first, Object second) {
		int cmp;
		<#list compareFields as cf>
		${cf}
		</#list>
		return 0;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		Object first = this.serializer.createInstance();
		Object second = this.serializer.createInstance();
		first = this.serializer.deserialize(first, firstSource);
		second = this.serializer.deserialize(second, secondSource);
		return this.compare(first, second);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return this.numLeadingNormalizableKeys > 0;
	}

	@Override
	public int getNormalizeKeyLen() {
		return this.normalizableKeyPrefixLen;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return this.numLeadingNormalizableKeys < this.numKeyFields ||
			this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||
			this.normalizableKeyPrefixLen > keyBytes;
	}

	@Override
	public void putNormalizedKey(Object value, MemorySegment target, int offset, int numBytes) {
		int len;
		do {
			<#list putNormalizedKeys as pnk>
			${pnk}
			</#list>
		} while (false);
	}

	@Override
	public boolean invertNormalizedKey() {
		return this.invertNormKey;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(Object record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object readWithKeyDenormalization(Object reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ${className} duplicate() {
		return new ${className}(this);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		int localIndex = index;
		<#list extractKeys as ek>
		${ek}
		</#list>
		return localIndex - index;
	}
}
