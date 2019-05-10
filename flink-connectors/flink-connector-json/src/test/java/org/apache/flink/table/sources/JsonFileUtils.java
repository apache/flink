/*
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
 */

package org.apache.flink.table.sources;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 *  use json data create file.
 */
public class JsonFileUtils {

	private static final String JSON_DATA = "{\"fruit\":[{\"weight\":8,\"type\":\"apple\"},{\"weight\":9,\"type\":\"pear\"}]," +
		"\"author\":\"J. R. R. Tolkien\",\"title\":\"The Lord of the Rings\",\"category\":\"fiction\",\"price\":22.99,\"isbn\":\"0-395-19395-8\"," +
		"\"email\":\"amy@only_for_json_udf_test.net\",\"owner\":\"amy\",\"zip_code\":\"94025\",\"fb_id\":\"1234\"}";
	private static TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static String createJsonFile() throws IOException {
		temporaryFolder.create();
		File output = temporaryFolder.newFile("test-data.json");
		FileWriter fileWriter = new FileWriter(output);
		fileWriter.write(JSON_DATA);
		fileWriter.close();
		return output.getPath();
	}

}
