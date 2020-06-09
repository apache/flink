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

package org.apache.flink.formats.avro;

import org.apache.flink.table.planner.runtime.stream.FsStreamingSinkITCaseBase;

import java.util.ArrayList;
import java.util.List;

/**
 * ITCase to test avro format for {@link AvroFileSystemFormatFactory} in stream mode.
 */
public class AvroFilesystemStreamITCase extends FsStreamingSinkITCaseBase {

	@Override
	public String[] additionalProperties() {
		List<String> ret = new ArrayList<>();
		ret.add("'format'='avro'");
		ret.add("'format.avro.codec'='snappy'");
		return ret.toArray(new String[0]);
	}
}
