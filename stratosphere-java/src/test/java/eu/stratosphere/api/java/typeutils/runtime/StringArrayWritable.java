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
package eu.stratosphere.api.java.typeutils.runtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StringArrayWritable implements Writable, Comparable<StringArrayWritable> {
	
	private String[] array = new String[0];
	
	public StringArrayWritable() {
		super();
	}
	
	public StringArrayWritable(String[] array) {
		this.array = array;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.array.length);
		
		for(String str : this.array) {
			byte[] b = str.getBytes();
			out.writeInt(b.length);
			out.write(b);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.array = new String[in.readInt()];
		
		for(int i = 0; i < this.array.length; i++) {
			byte[] b = new byte[in.readInt()];
			in.readFully(b);
			this.array[i] = new String(b);
		}
	}
	
	@Override
	public int compareTo(StringArrayWritable o) {
		if(this.array.length != o.array.length) {
			return this.array.length - o.array.length;
		}
		
		for(int i = 0; i < this.array.length; i++) {
			int comp = this.array[i].compareTo(o.array[i]);
			if(comp != 0) {
				return comp;
			}
		}
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof StringArrayWritable)) {
			return false;
		}
		return this.compareTo((StringArrayWritable) obj) == 0;
	}
}