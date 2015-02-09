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

package org.apache.flink.runtime.jobmanager;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

public class InputSplitWrapper implements IOReadableWritable {

	private InputSplit split;
	
	private byte[] splitData;
	
	// ------------------------------------------------------------------------
	
	public InputSplitWrapper() {}
	
	public InputSplitWrapper(InputSplit split) throws Exception {
		this.split = split;
		this.splitData = InstantiationUtil.serializeObject(split);
	}

	public InputSplit getSplit(ClassLoader userCodeClassLoader) throws ClassNotFoundException, IOException {
		if (split == null) {
			if (splitData == null) {
				throw new IllegalStateException("No split or split data available");
			}
			
			split = (InputSplit) InstantiationUtil.deserializeObject(splitData, userCodeClassLoader);
		}
		
		return split;
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public void read(DataInputView in) throws IOException {
		int len = in.readInt();
		splitData = new byte[len];
		in.readFully(splitData);
		split = null;
	}
	
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(splitData.length);
		out.write(splitData);		
	}
}
