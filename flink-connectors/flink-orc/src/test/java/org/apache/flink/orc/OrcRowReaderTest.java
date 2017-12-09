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

package org.apache.flink.orc;

import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link OrcRowReader} using example datasets from Apache Orc distribution and other sources.
 */
@RunWith(Parameterized.class)
public class OrcRowReaderTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(OrcRowReaderTest.class);
	private final String orcFile;
	private final long expectedRecordCount;

	public OrcRowReaderTest(String orcFile, long expectedRecordCount) {
		this.orcFile = orcFile;
		this.expectedRecordCount = expectedRecordCount;
	}

	@Parameterized.Parameters
	public static Iterable<Object[]> parameters() {
		return Arrays.asList(new Object[][]{

			// example datasets from Orc distribution
			{"examples/TestOrcFile.testMemoryManagementV12.orc", 2500L},
			{"examples/demo-12-zlib.orc", 1920800L},
			{"examples/TestOrcFile.metaData.orc", 1L},
			{"examples/over1k_bloom.orc", 2098L},
			{"examples/TestOrcFile.testDate2038.orc", 212000L},
			{"examples/TestOrcFile.testStringAndBinaryStatistics.orc", 4L},
			{"examples/TestOrcFile.test1.orc", 2L},
			{"examples/orc-file-11-format.orc", 7500L},
			{"examples/decimal.orc", 6000L},
			{"examples/TestOrcFile.columnProjection.orc", 21000L},
			{"examples/TestOrcFile.testWithoutIndex.orc", 50000L},
			{"examples/TestOrcFile.testStripeLevelStats.orc", 11000L},
			{"examples/TestVectorOrcFile.testLzo.orc", 10000L},
			{"examples/orc_split_elim.orc", 25000L},
			{"examples/TestOrcFile.testMemoryManagementV11.orc", 2500L},
			{"examples/demo-11-zlib.orc", 1920800L},
			{"examples/TestOrcFile.testPredicatePushdown.orc", 3500L},
			{"examples/TestOrcFile.emptyFile.orc", 0},
			{"examples/orc_split_elim_new.orc", 25000L},
			{"examples/demo-11-none.orc", 1920800L},
			{"examples/TestVectorOrcFile.testLz4.orc", 10000L},
			{"examples/TestOrcFile.testDate1900.orc", 70000L},
			{"examples/zero.orc", 0},
			{"examples/TestOrcFile.testSeek.orc", 32768L},
			{"examples/nulls-at-end-snappy.orc", 70000L},
			{"examples/TestOrcFile.testSnappy.orc", 10000L},
			// {"examples/version1999.orc", 0L},
			// {"examples/TestOrcFile.testTimestamp.orc", 0L},
			// {"examples/TestOrcFile.testUnionAndTimestamp.orc", 5077L},

			// activity streams like dataset
			{"activities/testdata.orc", 438L},
		});
	}

	@Test
	public void testDeserialization() throws Exception {
		LOGGER.info("Deserialization test on {}", orcFile);
		String orcPath = getPath(orcFile);
		Reader reader = OrcFile.createReader(new Path(orcPath), OrcFile.readerOptions(new Configuration()));
		TypeDescription schema = reader.getSchema();
		int[] selectedFields = createIdentityProjection(schema);
		RecordReader recordReader = reader.rows();

		OrcRowReader subject = new OrcRowReader(schema, selectedFields, recordReader);

		long actualRecordCount = 0;
		while (!subject.reachedEnd()) {
			Row row = subject.nextRecord(null);
			actualRecordCount++;
		}
		assertEquals(expectedRecordCount, actualRecordCount);
		subject.close();
	}

	private String getPath(String fileName) {
		ClassLoader classLoader = getClass().getClassLoader();
		URL url = Objects.requireNonNull(classLoader.getResource(fileName), String.format("Resource '%s' not available", fileName));
		return url.getPath();
	}

	private int[] createIdentityProjection(TypeDescription schema) {
		List<TypeDescription> children = schema.getChildren();
		int numChildren = children.size();
		return IntStream.range(0, numChildren).toArray();
	}
}
