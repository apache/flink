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

package org.apache.flink.streaming.connectors.hbase.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Suite of utility methods for HBase.
 */
public class HBaseUtils {

	public static byte[] serialize(TypeInformation<?> typeInfo, Object obj) {
		Class clazz = typeInfo.getTypeClass();
		if (byte[].class.equals(clazz)) {
			return (byte[]) obj;
		} else if (String.class.equals(clazz)) {
			return Bytes.toBytes((String) obj);
		} else if (Byte.class.equals(clazz)) {
			return Bytes.toBytes((Byte) obj);
		} else if (Short.class.equals(clazz)) {
			return Bytes.toBytes((Short) obj);
		} else if (Integer.class.equals(clazz)) {
			return Bytes.toBytes((Integer) obj);
		} else if (Long.class.equals(clazz)) {
			return Bytes.toBytes((Long) obj);
		} else if (Float.class.equals(clazz)) {
			return Bytes.toBytes((Float) obj);
		} else if (Double.class.equals(clazz)) {
			return Bytes.toBytes((Double) obj);
		} else if (Boolean.class.equals(clazz)) {
			return Bytes.toBytes((Boolean) obj);
		} else if (Timestamp.class.equals(clazz)) {
			return Bytes.toBytes(((Timestamp) obj).getTime());
		} else if (Date.class.equals(clazz)) {
			return Bytes.toBytes(((Date) obj).getTime());
		} else if (Time.class.equals(clazz)) {
			return Bytes.toBytes(((Time) obj).getTime());
		} else if (BigDecimal.class.equals(clazz)) {
			return Bytes.toBytes((BigDecimal) obj);
		} else if (BigInteger.class.equals(clazz)) {
			return ((BigInteger) obj).toByteArray();
		} else {
			throw new RuntimeException("Unsupported type " + clazz.getName());
		}
	}

	public static Connection createConnection(Map<String, String> hbaseConfig) throws IOException {
		Configuration configuration = new Configuration();
		for (Map.Entry<String, String> entry: hbaseConfig.entrySet()) {
			configuration.set(entry.getKey(), entry.getValue());
		}
		return ConnectionFactory.createConnection(configuration);
	}

	public static Table createTable(Connection connection, String tableName) throws IOException {
		try (Table table = connection.getTable(TableName.valueOf(tableName));
			Admin admin = connection.getAdmin()) {
			if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
				throw new IOException("Table is not available.");
			}
			return table;
		}
	}
}
