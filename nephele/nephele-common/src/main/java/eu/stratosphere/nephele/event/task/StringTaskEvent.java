/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

/*
 *  Copyright 2010 casp.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package eu.stratosphere.nephele.event.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class provides a simple Implementation of an event that holds a String
 * value.
 * 
 * @author casp
 */
public class StringTaskEvent extends AbstractTaskEvent {

	private String message = null;

	public StringTaskEvent() {
		// default constructor implementation.
		// should only be used for deserialization
	}

	public StringTaskEvent(String message) {
		this.message = message;
	}

	/**
	 * returns the stored String
	 * 
	 * @return
	 */
	public String getString() {
		return this.message;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (this.message == null) {
			out.writeInt(0);
		} else {
			byte[] stringbytes = this.message.getBytes("UTF8");
			out.writeInt(stringbytes.length);
			out.write(stringbytes);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		final int length = in.readInt();
		if (length > 0) {
			byte[] stringbytes = new byte[length];
			in.readFully(stringbytes, 0, length);
			String message = new String(stringbytes, "UTF8");
			this.message = message;
		} else {
			this.message = null;
		}
	}
}
