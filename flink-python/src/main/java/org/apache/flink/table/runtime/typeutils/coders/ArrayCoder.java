/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;

/**
 * A {@link Coder} for K[].
 *
 * @param <K> type of element.
 */
public class ArrayCoder<K> extends Coder<K[]> {

	private static final long serialVersionUID = 1L;

	private final Coder<K> elementCoder;

	private final Class<?> type;

	public static <K> ArrayCoder<K> of(Coder<K> elementCoder, Class<?> type) {
		return new ArrayCoder<>(elementCoder, type);
	}

	private ArrayCoder(Coder<K> elementCoder, Class<?> type) {
		this.elementCoder = elementCoder;
		this.type = type;
	}

	@Override
	public void encode(K[] value, OutputStream outStream) throws IOException {
		if (value == null) {
			throw new CoderException("cannot encode a null Array");
		}
		VarInt.encode(value.length, outStream);
		for (K ele : value) {
			elementCoder.encode(ele, outStream);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public K[] decode(InputStream inStream) throws IOException {
		int len = VarInt.decodeInt(inStream);
		K[] arr = (K[]) Array.newInstance(type, len);
		for (int i = 0; i < len; i++) {
			arr[i] = elementCoder.decode(inStream);
		}
		return arr;
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return Collections.singletonList(elementCoder);
	}

	@Override
	public void verifyDeterministic() {
	}
}
