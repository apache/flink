/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestDataUtilTest {

	@SuppressWarnings("resource")
	public boolean compareFile(String file1, String file2) throws FileNotFoundException,
			IOException {

		BufferedReader myInput1 = new BufferedReader(new InputStreamReader(new FileInputStream(
				file1)));
		BufferedReader myInput2 = new BufferedReader(new InputStreamReader(new FileInputStream(
				file2)));

		String line1, line2;
		while ((line1 = myInput1.readLine()) != null && (line2 = myInput2.readLine()) != null) {
			if (!line1.equals(line2))
				return false;
		}
		return true;
	}
}