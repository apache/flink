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

package eu.stratosphere.runtime.fs.hdfs;

import java.io.IOException;

import eu.stratosphere.core.fs.FSDataOutputStream;

public final class DistributedDataOutputStream extends FSDataOutputStream {

	private org.apache.hadoop.fs.FSDataOutputStream fdos;

	public DistributedDataOutputStream(org.apache.hadoop.fs.FSDataOutputStream fdos) {
		this.fdos = fdos;
	}

	@Override
	public void write(int b) throws IOException {

		fdos.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		fdos.write(b, off, len);
	}

	@Override
	public void close() throws IOException {
		fdos.close();
	}

}
