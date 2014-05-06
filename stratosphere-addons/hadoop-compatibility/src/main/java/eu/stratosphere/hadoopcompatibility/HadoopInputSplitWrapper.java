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

package eu.stratosphere.hadoopcompatibility;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.JobConf;

import eu.stratosphere.core.io.InputSplit;


public class HadoopInputSplitWrapper implements InputSplit {

	public transient org.apache.hadoop.mapred.InputSplit hadoopInputSplit;
	public JobConf jobConf;
	private int splitNumber;
	private String hadoopInputSplitTypeName;
	
	
	public org.apache.hadoop.mapred.InputSplit getHadoopInputSplit() {
		return hadoopInputSplit;
	}
	
	
	public HadoopInputSplitWrapper() {
		super();
	}
	
	
	public HadoopInputSplitWrapper(org.apache.hadoop.mapred.InputSplit hInputSplit, JobConf jobconf) {
		this.hadoopInputSplit = hInputSplit;
		this.hadoopInputSplitTypeName = hInputSplit.getClass().getCanonicalName();
		this.jobConf=jobconf;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(splitNumber);
		out.writeUTF(hadoopInputSplitTypeName);
		hadoopInputSplit.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
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
		this.hadoopInputSplit.readFields(in);
	}

	@Override
	public int getSplitNumber() {
		return this.splitNumber;
	}

	public void setSplitNumber(int splitNumber) {
		this.splitNumber = splitNumber;
	}
	
	public void setHadoopInputSplit(
			org.apache.hadoop.mapred.InputSplit hadoopInputSplit) {
		this.hadoopInputSplit = hadoopInputSplit;
	}
}
