/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.functions.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.ConfigConstants;

/**
 * Utility function to deserialize strings, used for CSV sinks.
 */
public class StringTupleDeserializerMap implements MapFunction<byte[], Tuple1<String>> {
	@Override
	public Tuple1<String> map(byte[] value) throws Exception {
		//5 = string type byte + string size
		return new Tuple1<>(new String(value, 5, value.length - 5, ConfigConstants.DEFAULT_CHARSET));
	}
}
