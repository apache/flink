/**
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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.BucketValidator;

/**
 * the format type for bucket file system ,
 * the row type is used to write  row-wise data,e.g. json or csv.
 * the bult type is used to write bulk-encoding data,e.g. Parquet or orc
 */
@Internal
public enum FormatType {
	ROW(BucketValidator.CONNECTOR_DATA_TYPE_ROW_VALUE),
	BULT(BucketValidator.CONNECTOR_DATA_TYPE_BULT_VALUE);

	private String type;

	FormatType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
