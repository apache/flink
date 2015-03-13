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

package org.apache.flink.api.java.hadoop.mapreduce.wrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * A wrapper that represents an input split from the Hadoop mapreduce API as
 * a Flink {@link InputSplit}.
 */
public class HadoopInputSplit extends LocatableInputSplit {
	
	private static final long serialVersionUID = 1L;
	
	public transient org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit;
	public transient JobContext jobContext;
	
	private int splitNumber;
	
	public org.apache.hadoop.mapreduce.InputSplit getHadoopInputSplit() {
		return mapreduceInputSplit;
	}
	
	
	public HadoopInputSplit() {
		super();
	}
	
	
	public HadoopInputSplit(int splitNumber, org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit, JobContext jobContext) {
		this.splitNumber = splitNumber;
		if(!(mapreduceInputSplit instanceof Writable)) {
			throw new IllegalArgumentException("InputSplit must implement Writable interface.");
		}
		this.mapreduceInputSplit = mapreduceInputSplit;
		this.jobContext = jobContext;
	}
	
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.splitNumber);
		out.writeUTF(this.mapreduceInputSplit.getClass().getName());
		Writable w = (Writable) this.mapreduceInputSplit;
		w.write(out);
	}
	
	@Override
	public void read(DataInputView in) throws IOException {
		this.splitNumber = in.readInt();
		String className = in.readUTF();
		
		if(this.mapreduceInputSplit == null) {
			try {
				Class<? extends org.apache.hadoop.io.Writable> inputSplit = 
						Class.forName(className).asSubclass(org.apache.hadoop.io.Writable.class);
				this.mapreduceInputSplit = (org.apache.hadoop.mapreduce.InputSplit) WritableFactories.newInstance(inputSplit);
			} catch (Exception e) {
				throw new RuntimeException("Unable to create InputSplit", e);
			}
		}
		((Writable)this.mapreduceInputSplit).readFields(in);
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeInt(this.splitNumber);
		out.writeUTF(this.mapreduceInputSplit.getClass().getName());
		Writable w = (Writable) this.mapreduceInputSplit;
		w.write(out);

	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.splitNumber=in.readInt();
		String className = in.readUTF();

		if(this.mapreduceInputSplit == null) {
			try {
				Class<? extends org.apache.hadoop.io.Writable> inputSplit =
						Class.forName(className).asSubclass(org.apache.hadoop.io.Writable.class);
				this.mapreduceInputSplit = (org.apache.hadoop.mapreduce.InputSplit) WritableFactories.newInstance(inputSplit);
			} catch (Exception e) {
				throw new RuntimeException("Unable to create InputSplit", e);
			}
		}
		((Writable)this.mapreduceInputSplit).readFields(in);
	}
	
	@Override
	public int getSplitNumber() {
		return this.splitNumber;
	}
	
	@Override
	public String[] getHostnames() {
		try {
			return this.mapreduceInputSplit.getLocations();
		} catch (IOException e) {
			return new String[0];
		} catch (InterruptedException e) {
			return new String[0];
		}
	}
}
