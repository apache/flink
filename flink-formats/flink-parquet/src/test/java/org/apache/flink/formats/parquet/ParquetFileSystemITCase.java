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

package org.apache.flink.formats.parquet;

import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;

/**
 * ITCase for {@link ParquetFileSystemFormatFactory}.
 */
@RunWith(Parameterized.class)
public class ParquetFileSystemITCase extends BatchFileSystemITCaseBase {

	private final boolean configure;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	public ParquetFileSystemITCase(boolean configure) {
		this.configure = configure;
	}

	@Override
	public String[] formatProperties() {
		List<String> ret = new ArrayList<>();
		ret.add("'format'='parquet'");
		if (configure) {
			ret.add("'parquet.utc-timezone'='true'");
			ret.add("'parquet.compression'='gzip'");
		}
		return ret.toArray(new String[0]);
	}

	@Override
	public void testNonPartition() {
		super.testNonPartition();

		// test configure success
		File directory = new File(URI.create(resultPath()).getPath());
		File[] files = directory.listFiles((dir, name) ->
				!name.startsWith(".") && !name.startsWith("_"));
		Assert.assertNotNull(files);
		Path path = new Path(URI.create(files[0].getAbsolutePath()));

		try {
			ParquetMetadata footer = readFooter(new Configuration(), path, range(0, Long.MAX_VALUE));
			if (configure) {
				Assert.assertEquals(
						"GZIP",
						footer.getBlocks().get(0).getColumns().get(0).getCodec().toString());
			} else {
				Assert.assertEquals(
						"UNCOMPRESSED",
						footer.getBlocks().get(0).getColumns().get(0).getCodec().toString());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
