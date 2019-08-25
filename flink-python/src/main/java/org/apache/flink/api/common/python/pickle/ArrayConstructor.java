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

package org.apache.flink.api.common.python.pickle;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Creates arrays of objects. Returns a primitive type array such as int[] if
 * the objects are ints, etc. Returns an ArrayList if it needs to
 * contain arbitrary objects (such as lists).
 */
public final class ArrayConstructor extends net.razorvine.pickle.objects.ArrayConstructor {

	@Override
	public Object construct(Object[] args) {
		if (args.length == 2 && args[1] instanceof String) {
			char typecode = ((String) args[0]).charAt(0);
			// This must be ISO 8859-1 / Latin 1, not UTF-8, to interoperate correctly
			byte[] data = ((String) args[1]).getBytes(StandardCharsets.ISO_8859_1);
			if (typecode == 'c') {
				// It seems like the pickle of pypy uses the similar protocol to Python 2.6, which uses
				// a string for array data instead of list as Python 2.7, and handles an array of
				// typecode 'c' as 1-byte character.
				char[] result = new char[data.length];
				int i = 0;
				while (i < data.length) {
					result[i] = (char) data[i];
					i += 1;
				}
				return result;
			}
		} else if (args.length == 2 && args[0] == "l") {
			// On Python 2, an array of typecode 'l' should be handled as long rather than int.
			ArrayList<Object> values = (ArrayList<Object>) args[1];
			long[] result = new long[values.size()];
			int i = 0;
			while (i < values.size()) {
				result[i] = ((Number) values.get(i)).longValue();
				i += 1;
			}
			return result;
		}

		return super.construct(args);
	}
}
