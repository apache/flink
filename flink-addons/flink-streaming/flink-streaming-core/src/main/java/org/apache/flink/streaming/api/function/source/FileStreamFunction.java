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

package org.apache.flink.streaming.api.function.source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.util.Collector;

public class FileStreamFunction implements SourceFunction<String> {
	private static final long serialVersionUID = 1L;

	private final String path;

	public FileStreamFunction(String path) {
		this.path = path;
	}

	@Override
	public void invoke(Collector<String> collector) throws IOException {
		while (true) {
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = br.readLine();
			while (line != null) {
				if (!line.equals("")) {
					collector.collect(line);
				}
				line = br.readLine();
			}
			br.close();
		}
	}
}
