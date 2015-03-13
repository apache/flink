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

package org.apache.flink.api.java.hadoop.mapred.wrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.JobConf;

/**
 * A wrapper that represents an input split from the Hadoop mapred API as
 * a Flink {@link InputSplit}.
 */
public class HadoopInputSplit extends LocatableInputSplit {

	private static final long serialVersionUID = 1L;
	
	private transient org.apache.hadoop.mapred.InputSplit hadoopInputSplit;
	
	private JobConf jobConf;
	
	private int splitNumber;
	private String hadoopInputSplitTypeName;


	public org.apache.hadoop.mapred.InputSplit getHadoopInputSplit() {
		return hadoopInputSplit;
	}

	public HadoopInputSplit() {
		super();
	}

	public HadoopInputSplit(int splitNumber, org.apache.hadoop.mapred.InputSplit hInputSplit, JobConf jobconf) {

		this.splitNumber = splitNumber;
		this.hadoopInputSplit = hInputSplit;
		this.hadoopInputSplitTypeName = hInputSplit.getClass().getName();
		this.jobConf = jobconf;

	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(splitNumber);
		out.writeUTF(hadoopInputSplitTypeName);
		jobConf.write(out);
		hadoopInputSplit.write(out);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.splitNumber = in.readInt();
		this.hadoopInputSplitTypeName = in.readUTF();
		if(hadoopInputSplit == null) {
			try {
				Class<? extends org.apache.hadoop.io.Writable> inputSplit =
						Class.forName(hadoopInputSplitTypeName).asSubclass(org.apache.hadoop.io.Writable.class);
				this.hadoopInputSplit = (org.apache.hadoop.mapred.InputSplit) WritableFactories.newInstance( inputSplit );
			}
			catch (Exception e) {
				throw new RuntimeException("Unable to create InputSplit", e);
			}
		}
		jobConf = new JobConf();
		jobConf.readFields(in);
		if (this.hadoopInputSplit instanceof Configurable) {
			((Configurable) this.hadoopInputSplit).setConf(this.jobConf);
		}
		this.hadoopInputSplit.readFields(in);

	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.writeInt(splitNumber);
		out.writeUTF(hadoopInputSplitTypeName);
		jobConf.write(out);
		hadoopInputSplit.write(out);

	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.splitNumber=in.readInt();
		this.hadoopInputSplitTypeName = in.readUTF();
		if(hadoopInputSplit == null) {
			try {
				Class<? extends org.apache.hadoop.io.Writable> inputSplit =
						Class.forName(hadoopInputSplitTypeName).asSubclass(org.apache.hadoop.io.Writable.class);
				this.hadoopInputSplit = (org.apache.hadoop.mapred.InputSplit) WritableFactories.newInstance( inputSplit );
			}
			catch (Exception e) {
				throw new RuntimeException("Unable to create InputSplit", e);
			}
		}
		jobConf = new JobConf();
		jobConf.readFields(in);
		if (this.hadoopInputSplit instanceof Configurable) {
			((Configurable) this.hadoopInputSplit).setConf(this.jobConf);
		}
		this.hadoopInputSplit.readFields(in);
	}

	@Override
	public int getSplitNumber() {
		return this.splitNumber;
	}

	@Override
	public String[] getHostnames() {
		try {
			return this.hadoopInputSplit.getLocations();
		} catch(IOException ioe) {
			return new String[0];
		}
	}
}
