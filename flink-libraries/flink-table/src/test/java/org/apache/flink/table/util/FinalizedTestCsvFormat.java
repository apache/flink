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

package org.apache.flink.table.util;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.sinks.csv.BaseRowCsvOutputFormat;

import java.io.File;
import java.io.IOException;


/**
 * A CSV Output Format for test purpose with finalized method.
 */
public class FinalizedTestCsvFormat extends BaseRowCsvOutputFormat implements FinalizeOnMaster {

	private final String markPath;

	public FinalizedTestCsvFormat(
			Path outputPath,
			InternalType[] fieldTypes,
			String markPath) {
		super(outputPath, fieldTypes);
		this.markPath = markPath;
	}

	@Override
	public void finalizeGlobal(int parallelism) throws IOException {
		File file = new File(markPath);
		if (file.createNewFile()) {
			file.deleteOnExit();
		}
	}
}
