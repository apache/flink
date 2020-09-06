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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;

/**
 * Provide type specialized setters to reduce if/else and eliminate box and unbox. This is mainly
 * used on the binary format such as {@link BinaryRowData}.
 */
@Internal
public interface TypedSetters {

	void setNullAt(int pos);

	void setBoolean(int pos, boolean value);

	void setByte(int pos, byte value);

	void setShort(int pos, short value);

	void setInt(int pos, int value);

	void setLong(int pos, long value);

	void setFloat(int pos, float value);

	void setDouble(int pos, double value);

	/**
	 * Set the decimal column value.
	 *
	 * <p>Note:
	 * Precision is compact: can call {@link #setNullAt} when decimal is null.
	 * Precision is not compact: can not call {@link #setNullAt} when decimal is null, must call
	 * {@code setDecimal(pos, null, precision)} because we need update var-length-part.
	 */
	void setDecimal(int pos, DecimalData value, int precision);

	/**
	 * Set Timestamp value.
	 *
	 * <p>Note:
	 * If precision is compact: can call {@link #setNullAt} when TimestampData value is null.
	 * Otherwise: can not call {@link #setNullAt} when TimestampData value is null, must call
	 * {@code setTimestamp(pos, null, precision)} because we need to update var-length-part.
	 */
	void setTimestamp(int pos, TimestampData value, int precision);
}
