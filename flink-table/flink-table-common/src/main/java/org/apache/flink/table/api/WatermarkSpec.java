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

package org.apache.flink.table.api;

import org.apache.flink.table.types.DataType;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Watermark metadata defined in {@link TableSchema}. It mainly contains 3 parts:
 *
 * <ol>
 *     <li>the rowtime attribute.</li>
 *     <li>the string representation of watermark generation expression.</li>
 *     <li>the data type of the computation result of watermark generation expression.</li>
 * </ol>
 */
public class WatermarkSpec {

	private final String rowtimeAttribute;

	private final String watermarkExpressionString;

	private final DataType watermarkExprOutputType;

	public WatermarkSpec(String rowtimeAttribute, String watermarkExpressionString, DataType watermarkExprOutputType) {
		this.rowtimeAttribute = checkNotNull(rowtimeAttribute);
		this.watermarkExpressionString = checkNotNull(watermarkExpressionString);
		this.watermarkExprOutputType = checkNotNull(watermarkExprOutputType);
	}

	/**
	 * Returns the name of rowtime attribute, it can be a nested field using dot separator.
	 * The referenced attribute must be present in the {@link TableSchema} and of
	 * type {@link org.apache.flink.table.types.logical.LogicalTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}.
	 */
	public String getRowtimeAttribute() {
		return rowtimeAttribute;
	}

	/**
	 * Returns the string representation of watermark generation expression.
	 * The string representation is a qualified SQL expression string (UDFs are expanded).
	 */
	public String getWatermarkExpr() {
		return watermarkExpressionString;
	}

	/**
	 * Returns the data type of the computation result of watermark generation expression.
	 */
	public DataType getWatermarkExprOutputType() {
		return watermarkExprOutputType;
	}

	public String asSummaryString() {
		return "WATERMARK FOR " + rowtimeAttribute + ": " + watermarkExprOutputType +
			" AS " + watermarkExpressionString;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WatermarkSpec that = (WatermarkSpec) o;
		return Objects.equals(rowtimeAttribute, that.rowtimeAttribute) &&
			Objects.equals(watermarkExpressionString, that.watermarkExpressionString) &&
			Objects.equals(watermarkExprOutputType, that.watermarkExprOutputType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowtimeAttribute, watermarkExpressionString, watermarkExprOutputType);
	}

	@Override
	public String toString() {
		return asSummaryString();
	}
}
