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

package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

import org.apache.arrow.vector.VarCharVector;

/**
 * {@link ArrowFieldReader} for VarChar.
 */
@Internal
public final class VarCharFieldReader extends ArrowFieldReader<String> {

	public VarCharFieldReader(VarCharVector varCharVector) {
		super(varCharVector);
	}

	@Override
	public String read(int index) {
		if (getValueVector().isNull(index)) {
			return null;
		} else {
			byte[] bytes = ((VarCharVector) getValueVector()).get(index);
			return StringUtf8Utils.decodeUTF8(bytes, 0, bytes.length);
		}
	}
}
