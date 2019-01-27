/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.vector;

import org.apache.flink.util.TimeConvertUtils;

/**
 * This class represents a nullable int column vector.
 * This class will be used for operations on all short types
 * The vector[] field is public by design for high-performance access in the inner
 * loop of query execution.
 */
public class StringColumnVector extends BytesColumnVector {
	private static final long serialVersionUID = -7719263797458113123L;

	public StringColumnVector(int len) {
		super(len);
	}

	@Override
	public Object get(int index) {
		return super.toString(index);
	}

	public void set(int index, Object object) {
		if (object == null) {
			isNull[index] = true;
			noNulls = false;
		} else {
			isNull[index] = false;
			byte[] bytes = ((String) object).getBytes(TimeConvertUtils.UTF_8);
			setVal(index, bytes, 0, bytes.length);
		}
	}

}
