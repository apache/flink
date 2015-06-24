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

package org.apache.flink.graph.test.example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.apache.flink.graph.example.MusicProfiles;
import org.apache.flink.graph.example.utils.MusicProfilesData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class MusicProfilesITCase extends MultipleProgramsTestBase {

	private String tripletsPath;

	private String mismatchesPath;

	private String topSongsResultPath;

	private String communitiesResultPath;

	private String expectedTopSongs;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public MusicProfilesITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Before
	public void before() throws Exception {
		topSongsResultPath = tempFolder.newFile().toURI().toString();
		communitiesResultPath = tempFolder.newFile().toURI().toString();

		File tripletsFile = tempFolder.newFile();
		Files.write(MusicProfilesData.USER_SONG_TRIPLETS, tripletsFile, Charsets.UTF_8);
		tripletsPath = tripletsFile.toURI().toString();

		File mismatchesFile = tempFolder.newFile();
		Files.write(MusicProfilesData.MISMATCHES, mismatchesFile, Charsets.UTF_8);
		mismatchesPath = mismatchesFile.toURI().toString();
	}

	@Test
	public void testMusicProfilesExample() throws Exception {
		MusicProfiles.main(new String[]{tripletsPath, mismatchesPath, topSongsResultPath, communitiesResultPath,
				MusicProfilesData.MAX_ITERATIONS + ""});
		expectedTopSongs = MusicProfilesData.TOP_SONGS_RESULT;
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expectedTopSongs, topSongsResultPath);

		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, communitiesResultPath, new String[]{}, false);

		String[] result = list.toArray(new String[list.size()]);
		Arrays.sort(result);

		// check that user_1 and user_2 are in the same community
		Assert.assertEquals("users 1 and 2 are not in the same community",
				result[0].substring(7), result[1].substring(7));

		// check that user_3, user_4 and user_5 are in the same community
		Assert.assertEquals("users 3 and 4 are not in the same community",
				result[2].substring(7), result[3].substring(7));
		Assert.assertEquals("users 4 and 5 are not in the same community",
				result[3].substring(7), result[4].substring(7));
	}
}