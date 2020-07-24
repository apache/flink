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
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * This class is used to adapt to Hive's legacy (2.0.x) timestamp column vector which is a LongColumnVector.
 * This class does not import TimestampColumnVector and provides utils to decide whether new or legacy column
 * vector should be used.
 */
public class OrcLegacyTimestampColumnVector extends AbstractOrcColumnVector implements
		org.apache.flink.table.data.vector.TimestampColumnVector {

	private static final Logger LOG = LoggerFactory.getLogger(OrcLegacyTimestampColumnVector.class);

	private static Class hiveTSColVectorClz;
	private static Constructor constructor;
	private static Method getTimeMethod;
	private static Method getNanosMethod;
	private static Method fillMethod;
	private static boolean hiveTSColVectorAvailable = false;

	static {
		try {
			hiveTSColVectorClz = Class.forName("org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector");
			constructor = hiveTSColVectorClz.getConstructor(int.class);
			getTimeMethod = hiveTSColVectorClz.getDeclaredMethod("getTime", int.class);
			getNanosMethod = hiveTSColVectorClz.getDeclaredMethod("getNanos", int.class);
			fillMethod = hiveTSColVectorClz.getDeclaredMethod("fill", Timestamp.class);
			hiveTSColVectorAvailable = true;
		} catch (ClassNotFoundException | NoSuchMethodException e) {
			LOG.debug("Hive TimestampColumnVector not available", e);
		}
	}

	private final ColumnVector hiveVector;

	OrcLegacyTimestampColumnVector(ColumnVector vector) {
		super(vector);
		if (isHiveTimestampColumnVector(vector) || vector instanceof LongColumnVector) {
			this.hiveVector = vector;
		} else {
			throw new IllegalArgumentException("Unsupported column vector type for timestamp: " + vector.getClass().getName());
		}
	}

	@Override
	public TimestampData getTimestamp(int i, int precision) {
		int index = hiveVector.isRepeating ? 0 : i;
		Timestamp timestamp;
		if (hiveTSColVectorAvailable) {
			timestamp = new Timestamp(getTime(index));
			timestamp.setNanos(getNanos(index));
		} else {
			timestamp = toTimestamp(((LongColumnVector) hiveVector).vector[index]);
		}
		return TimestampData.fromTimestamp(timestamp);
	}

	private long getTime(int index) {
		try {
			return (long) getTimeMethod.invoke(hiveVector, index);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	private int getNanos(int index) {
		try {
			return (int) getNanosMethod.invoke(hiveVector, index);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	private void fill(Timestamp timestamp) {
		try {
			if (hiveTSColVectorAvailable) {
				fillMethod.invoke(hiveVector, timestamp);
			} else {
				((LongColumnVector) hiveVector).fill(fromTimestamp(timestamp));
			}
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	static boolean isHiveTimestampColumnVector(ColumnVector vector) {
		return hiveTSColVectorAvailable && hiveTSColVectorClz.isAssignableFrom(vector.getClass());
	}

	// creates a Hive ColumnVector of constant timestamp value
	static ColumnVector createFromConstant(int batchSize, Object value) {
		OrcLegacyTimestampColumnVector columnVector = new OrcLegacyTimestampColumnVector(createHiveColumnVector(batchSize));
		if (value == null) {
			columnVector.hiveVector.noNulls = false;
			columnVector.hiveVector.isNull[0] = true;
			columnVector.hiveVector.isRepeating = true;
		} else {
			Timestamp timestamp = value instanceof LocalDateTime ?
					Timestamp.valueOf((LocalDateTime) value) : (Timestamp) value;
			columnVector.fill(timestamp);
			columnVector.hiveVector.isNull[0] = false;
		}
		return columnVector.hiveVector;
	}

	private static ColumnVector createHiveColumnVector(int len) {
		try {
			if (hiveTSColVectorAvailable) {
				return (ColumnVector) constructor.newInstance(len);
			} else {
				return new LongColumnVector(len);
			}
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	// converting from/to Timestamp is copied from Hive 2.0.0 TimestampUtils
	private static long fromTimestamp(Timestamp timestamp) {
		long time = timestamp.getTime();
		int nanos = timestamp.getNanos();
		return (time * 1000000) + (nanos % 1000000);
	}

	private static Timestamp toTimestamp(long timeInNanoSec) {
		long integralSecInMillis = (timeInNanoSec / 1000000000) * 1000; // Full seconds converted to millis.
		long nanos = timeInNanoSec % 1000000000; // The nanoseconds.
		if (nanos < 0) {
			nanos = 1000000000 + nanos; // The positive nano-part that will be added to milliseconds.
			integralSecInMillis = ((timeInNanoSec / 1000000000) - 1) * 1000; // Reduce by one second.
		}
		Timestamp res = new Timestamp(0);
		res.setTime(integralSecInMillis);
		res.setNanos((int) nanos);
		return res;
	}
}
