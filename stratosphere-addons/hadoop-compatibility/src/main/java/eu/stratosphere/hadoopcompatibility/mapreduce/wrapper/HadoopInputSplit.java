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

package eu.stratosphere.hadoopcompatibility.mapreduce.wrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapreduce.JobContext;

import eu.stratosphere.core.io.InputSplit;


public class HadoopInputSplit implements InputSplit {
	
	public transient org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit;
	public transient JobContext jobContext;
	
	private int splitNumber;	
	
	public org.apache.hadoop.mapreduce.InputSplit getHadoopInputSplit() {
		return mapreduceInputSplit;
	}
	
	
	public HadoopInputSplit() {
		super();
	}
	
	
	public HadoopInputSplit(org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit, JobContext jobContext) {
		if(!(mapreduceInputSplit instanceof Writable)) {
			throw new IllegalArgumentException("InputSplit must implement Writable interface.");
		}
		this.mapreduceInputSplit = mapreduceInputSplit;
		this.jobContext = jobContext;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.splitNumber);
		out.writeUTF(this.mapreduceInputSplit.getClass().getName());
		Writable w = (Writable) this.mapreduceInputSplit;
		w.write(out);
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		this.splitNumber=in.readInt();
		String className = in.readUTF();
		
		if(this.mapreduceInputSplit == null) {
			try {
				Class<? extends org.apache.hadoop.io.Writable> inputSplit = 
						Class.forName(className).asSubclass(org.apache.hadoop.io.Writable.class);
				this.mapreduceInputSplit = (org.apache.hadoop.mapreduce.InputSplit) WritableFactories.newInstance(inputSplit);
			}
			catch (Exception e) {
				throw new RuntimeException("Unable to create InputSplit", e);
			}
		}
		((Writable)this.mapreduceInputSplit).readFields(in);
	}
	
	@Override
	public int getSplitNumber() {
		return this.splitNumber;
	}
	
	public void setSplitNumber(int splitNumber) {
		this.splitNumber = splitNumber;
	}
}
