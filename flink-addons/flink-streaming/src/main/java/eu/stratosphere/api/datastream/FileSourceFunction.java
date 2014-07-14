/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.datastream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class FileSourceFunction extends SourceFunction<Tuple1<String>> {
	private static final long serialVersionUID = 1L;
	
	private final String path;
	private Tuple1<String> outTuple = new Tuple1<String>();
	
	public FileSourceFunction(String path) {
		this.path = path;
	}
	
	@Override
	public void invoke(Collector<Tuple1<String>> collector) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = br.readLine();
		while (line != null) {
			if (line != "") {
				outTuple.f0 = line;
				collector.collect(outTuple);
			}
			line = br.readLine();
		}
		br.close();
	}

}
