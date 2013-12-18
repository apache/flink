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

package eu.stratosphere.addons.hbase.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

import eu.stratosphere.types.Value;

public class HBaseResult implements Value {
	
	private static final long serialVersionUID = 1L;

	private Result result;
	
	
	public HBaseResult() {
		this.result = new Result();
	}
	
	public HBaseResult(Result result) {
		this.result = result;
	}
	
	
	public Result getResult() {
		return this.result;
	}
	
	public void setResult(Result result) {
		this.result = result;
	}
	
	public String getStringData() {
		if(this.result != null) {
			return this.result.toString();
		}
		return null;
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		this.result.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.result.write(out);	
	}
}
