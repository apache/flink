/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.test.checkpointing;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.junit.Assert;
import org.junit.Test;

public class TimestampedFileInputSplitTest {

	@Test
	public void testSplitEquality() {

		TimestampedFileInputSplit eos1 = TimestampedFileInputSplit.EOS;
		TimestampedFileInputSplit eos2 = TimestampedFileInputSplit.EOS;

		Assert.assertEquals(eos1, eos2);

		TimestampedFileInputSplit richFirstSplit =
			new TimestampedFileInputSplit(10, 2, new Path("test"), 0, 100, null);
		Assert.assertNotEquals(eos1, richFirstSplit);

		TimestampedFileInputSplit richSecondSplit =
			new TimestampedFileInputSplit(10, 2, new Path("test"), 0, 100, null);
		Assert.assertEquals(richFirstSplit, richSecondSplit);

		TimestampedFileInputSplit richModSecondSplit =
			new TimestampedFileInputSplit(11, 2, new Path("test"), 0, 100, null);
		Assert.assertNotEquals(richSecondSplit, richModSecondSplit);

		TimestampedFileInputSplit richThirdSplit =
			new TimestampedFileInputSplit(10, 2, new Path("test/test1"), 0, 100, null);
		Assert.assertEquals(richThirdSplit.getModificationTime(), 10);
		Assert.assertNotEquals(richFirstSplit, richThirdSplit);

		TimestampedFileInputSplit richThirdSplitCopy =
			new TimestampedFileInputSplit(10, 2, new Path("test/test1"), 0, 100, null);
		Assert.assertEquals(richThirdSplitCopy, richThirdSplit);
	}

	@Test
	public void testSplitComparison() {
		TimestampedFileInputSplit richFirstSplit =
			new TimestampedFileInputSplit(10, 3, new Path("test/test1"), 0, 100, null);

		TimestampedFileInputSplit richSecondSplit =
			new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 0, 100, null);

		TimestampedFileInputSplit richThirdSplit =
			new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

		TimestampedFileInputSplit richForthSplit =
			new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

		// lexicographically on the path order
		Assert.assertTrue(richFirstSplit.compareTo(richSecondSplit) < 0);
		Assert.assertTrue(richFirstSplit.compareTo(richThirdSplit) < 0);

		// same mod time, same file so smaller split number first
		Assert.assertTrue(richThirdSplit.compareTo(richSecondSplit) < 0);

		// smaller modification time first
		Assert.assertTrue(richThirdSplit.compareTo(richForthSplit) < 0);
	}

	@Test
	public void testIllegalArgument() {
		try {
			new TimestampedFileInputSplit(-10, 2, new Path("test"), 0, 100, null); // invalid modification time
		} catch (Exception e) {
			if (!(e instanceof IllegalArgumentException)) {
				Assert.fail(e.getMessage());
			}
		}
	}
}
