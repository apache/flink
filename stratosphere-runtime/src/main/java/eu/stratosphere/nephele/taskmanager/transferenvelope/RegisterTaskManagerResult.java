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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.util.EnumUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RegisterTaskManagerResult implements IOReadableWritable {
	public enum ReturnCode{
		SUCCESS, FAILURE
	};

	public RegisterTaskManagerResult(){
		this.returnCode = ReturnCode.SUCCESS;
	}

	public RegisterTaskManagerResult(ReturnCode returnCode){
		this.returnCode = returnCode;
	}

	private ReturnCode returnCode;

	public ReturnCode getReturnCode() { return this.returnCode; }


	@Override
	public void write(DataOutput out) throws IOException {
		EnumUtils.writeEnum(out, this.returnCode);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.returnCode = EnumUtils.readEnum(in, ReturnCode.class);
	}
}
