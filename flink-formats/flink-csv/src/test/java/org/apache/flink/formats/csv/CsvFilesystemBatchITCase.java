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

package org.apache.flink.formats.csv;

import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ITCase to test csv format for {@link CsvFileSystemFormatFactory} in batch mode.
 */
@RunWith(Enclosed.class)
public class CsvFilesystemBatchITCase {

	/**
	 * General IT cases for CsvRowDataFilesystem in batch mode.
	 */
	public static class GeneralCsvFilesystemBatchITCase extends BatchFileSystemITCaseBase {

		@Override
		public String[] formatProperties() {
			List<String> ret = new ArrayList<>();
			ret.add("'format'='csv'");
			ret.add("'format.field-delimiter'=';'");
			ret.add("'format.quote-character'='#'");
			return ret.toArray(new String[0]);
		}
	}

	/**
	 * Enriched IT cases that including testParseError and testEscapeChar for CsvRowDataFilesystem in batch mode.
	 */
	public static class EnrichedCsvFilesystemBatchITCase extends BatchFileSystemITCaseBase {

		@Override
		public String[] formatProperties() {
			List<String> ret = new ArrayList<>();
			ret.add("'format'='csv'");
			ret.add("'format.ignore-parse-errors'='true'");
			ret.add("'format.escape-character'='\t'");
			return ret.toArray(new String[0]);
		}

		@Test
		public void testParseError() throws Exception {
			String path = new URI(resultPath()).getPath();
			new File(path).mkdirs();
			File file = new File(path, "test_file");
			file.createNewFile();
			FileUtils.writeFileUtf8(file,
				"x5,5,1,1\n" +
					"x5,5,2,2,2\n" +
					"x5,5,1,1");

			check("select * from nonPartitionedTable",
				Arrays.asList(
					Row.of("x5,5,1,1"),
					Row.of("x5,5,1,1")));
		}

		@Test
		public void testEscapeChar() throws Exception {
			String path = new URI(resultPath()).getPath();
			new File(path).mkdirs();
			File file = new File(path, "test_file");
			file.createNewFile();
			FileUtils.writeFileUtf8(file,
				"x5,\t\n5,1,1\n" +
					"x5,\t5,2,2");

			check("select * from nonPartitionedTable",
				Arrays.asList(
					Row.of("x5,5,1,1"),
					Row.of("x5,5,2,2")));
		}
	}
}
