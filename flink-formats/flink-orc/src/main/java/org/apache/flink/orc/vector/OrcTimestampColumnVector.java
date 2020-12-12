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

package org.apache.flink.orc.vector;

import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * This column vector is used to adapt hive's TimestampColumnVector to
 * Flink's TimestampColumnVector.
 */
public class OrcTimestampColumnVector extends AbstractOrcColumnVector implements
		org.apache.flink.table.data.vector.TimestampColumnVector {

	private TimestampColumnVector vector;

	public OrcTimestampColumnVector(ColumnVector vector) {
		super(vector);
		this.vector = (TimestampColumnVector) vector;
	}

	@Override
	public TimestampData getTimestamp(int i, int precision) {
		int index = vector.isRepeating ? 0 : i;
		Timestamp timestamp = new Timestamp(vector.time[index]);
		timestamp.setNanos(vector.nanos[index]);
		return TimestampData.fromTimestamp(timestamp);
	}

	public static ColumnVector createFromConstant(int batchSize, Object value) {
		TimestampColumnVector res = new TimestampColumnVector(batchSize);
		if (value == null) {
			res.noNulls = false;
			res.isNull[0] = true;
			res.isRepeating = true;
		} else {
			Timestamp timestamp = value instanceof LocalDateTime ?
					Timestamp.valueOf((LocalDateTime) value) : (Timestamp) value;
			res.fill(timestamp);
			res.isNull[0] = false;
		}
		return res;
	}
}
