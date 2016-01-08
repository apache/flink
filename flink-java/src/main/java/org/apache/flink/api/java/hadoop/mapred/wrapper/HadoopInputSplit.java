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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

/**
 * A wrapper that represents an input split from the Hadoop mapred API as
 * a Flink {@link InputSplit}.
 */
public class HadoopInputSplit extends LocatableInputSplit {

	private static final long serialVersionUID = -6990336376163226160L;
	
	
	private final Class<? extends org.apache.hadoop.mapred.InputSplit> splitType;
	
	private transient JobConf jobConf;

	private transient org.apache.hadoop.mapred.InputSplit hadoopInputSplit;
	
	
	public HadoopInputSplit(int splitNumber, org.apache.hadoop.mapred.InputSplit hInputSplit, JobConf jobconf) {
		super(splitNumber, (String) null);

		if (hInputSplit == null) {
			throw new NullPointerException("Hadoop input split must not be null");
		}
		if (jobconf == null) {
			throw new NullPointerException("Hadoop JobConf must not be null");
		}
		
		this.splitType = hInputSplit.getClass();

		this.jobConf = jobconf;
		this.hadoopInputSplit = hInputSplit;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	@Override
	public String[] getHostnames() {
		try {
			return this.hadoopInputSplit.getLocations();
		}
		catch(IOException e) {
			return new String[0];
		}
	}
	
	public org.apache.hadoop.mapred.InputSplit getHadoopInputSplit() {
		return hadoopInputSplit;
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		// serialize the parent fields and the final fields
		out.defaultWriteObject();

		// the job conf knows how to serialize itself
		jobConf.write(out);
		
		// write the input split
		hadoopInputSplit.write(out);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		// read the parent fields and the final fields
		in.defaultReadObject();

		// the job conf knows how to deserialize itself
		jobConf = new JobConf();
		jobConf.readFields(in);
		
		
		try {
			hadoopInputSplit = (org.apache.hadoop.mapred.InputSplit) WritableFactories.newInstance(splitType);
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to instantiate Hadoop InputSplit", e);
		}

		if (hadoopInputSplit instanceof Configurable) {
			((Configurable) hadoopInputSplit).setConf(this.jobConf);
		}
		else if (hadoopInputSplit instanceof JobConfigurable) {
			((JobConfigurable) hadoopInputSplit).configure(this.jobConf);
		}
		hadoopInputSplit.readFields(in);
	}
}
