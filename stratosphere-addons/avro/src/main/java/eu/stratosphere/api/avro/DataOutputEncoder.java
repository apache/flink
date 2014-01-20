/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.avro;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;


public class DataOutputEncoder extends BinaryEncoder {
	
	private DataOutput out;
	
	
	public void setOut(DataOutput out) {
		this.out = out;
	}


	@Override
	public void flush() throws IOException {}

	@Override
	public int bytesBuffered() {
		return 0;
	}

	@Override
	public void writeBoolean(boolean b) throws IOException {
		out.writeBoolean(b);
	}

	@Override
	public void writeInt(int n) throws IOException {
		out.writeInt(n);
	}

	@Override
	public void writeLong(long n) throws IOException {
		out.writeLong(n);
	}

	@Override
	public void writeFloat(float f) throws IOException {
		out.writeFloat(f);
	}

	@Override
	public void writeDouble(double d) throws IOException {
		out.writeDouble(d);
	}

	@Override
	public void writeFixed(byte[] bytes, int start, int len) throws IOException {
		out.write(bytes, start, len);
	}
	
	@Override
	protected void writeZero() throws IOException {
		out.writeInt(0);
	}

}
