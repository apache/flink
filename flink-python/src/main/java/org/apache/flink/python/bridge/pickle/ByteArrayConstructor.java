/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.bridge.pickle;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Creates byte arrays (byte[]). Deal with an empty byte array pickled by Python 3.
 */
public final class ByteArrayConstructor extends net.razorvine.pickle.objects.ByteArrayConstructor {

	@Override
	public Object construct(Object[] args) {
		if (args.length == 0) {
			return ArrayUtils.EMPTY_BYTE_ARRAY;
		} else {
			return super.construct(args);
		}
	}
}
