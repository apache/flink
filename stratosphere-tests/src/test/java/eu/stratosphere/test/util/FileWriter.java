/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.util;

import java.io.File;
import java.io.PrintWriter;

/**
 * @author Erik Nijkamp
 */
public class FileWriter {
	private PrintWriter out;

	private String path = "";

	public FileWriter() {

	}

	public FileWriter dir(String dir) throws Exception {
		path += dir + "/";
		new File(path).mkdirs();
		return this;
	}

	public FileWriter file(String file) throws Exception {
		out = new PrintWriter(new File(path + file));
		return this;
	}

	public FileWriter write(String... lines) {
		for (String line : lines)
			out.println(line);
		return this;
	}

	public FileWriter close() {
		out.close();
		return this;
	}
}
