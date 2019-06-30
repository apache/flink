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

package org.apache.flink.connectors.hbase.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

/**
 * HBaseBytesSerializer gives support to serialize into or deserialize from HBase bytes data.
 * Currently only support simple java data types, include:
 * byte[], String, Byte, Short, Integer, Long, Float, Double, Boolean, Timestamp, Date, Time, BigDecimal
 */
public class HBaseBytesSerializer implements Serializable {
	String stringCharset;
	int typeIdx;

	public HBaseBytesSerializer(TypeInformation typeInfo) {
		this(typeInfo, HBaseTypeUtils.DEFAULT_CHARSET);
	}

	public HBaseBytesSerializer(TypeInformation typeInfo, String charset) {
		this.typeIdx = HBaseTypeUtils.getTypeIndex(typeInfo);
		this.stringCharset = charset;
	}

	public byte[] toHBaseBytes(Object value) throws UnsupportedEncodingException {
		return HBaseTypeUtils.serializeFromObject(value, typeIdx, stringCharset);
	}

	public Object fromHBaseBytes(byte[] value) throws UnsupportedEncodingException {
		return HBaseTypeUtils.deserializeToObject(value, typeIdx, stringCharset);
	}
}
