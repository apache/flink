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
package eu.stratosphere.api.java.io;

import java.io.PrintStream;

import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.configuration.Configuration;


public class PrintingOutputFormat<T> implements OutputFormat<T> {

	private static final long serialVersionUID = 1L;

	private static final boolean STD_OUT = false;
	private static final boolean STD_ERR = true;
	
	
	private boolean target; 
	
	private transient PrintStream stream;
	
	private transient String prefix;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Instantiates a printing output format that prints to standard out.
	 */
	public PrintingOutputFormat() {}
	
	/**
	 * Instantiates a printing output format that prints to standard out.
	 * 
	 * @param stdErr True, if the format should print to standard error instead of standard out.
	 */
	public PrintingOutputFormat(boolean stdErr) {
		this.target = stdErr;
	}
	
	
	public void setTargetToStandardOut() {
		this.target = STD_OUT;
	}
	
	public void setTargetToStandardErr() {
		this.target = STD_ERR;
	}	
	
	
	@Override
	public void configure(Configuration parameters) {}


	@Override
	public void open(int taskNumber, int numTasks) {
		// get the target stream
		this.stream = this.target == STD_OUT ? System.out : System.err;
		
		// set the prefix if we have a >1 DOP
		this.prefix = (numTasks > 1) ? (taskNumber + "> ") : null;
	}

	@Override
	public void writeRecord(T record) {
		if (this.prefix != null) {
			this.stream.println(this.prefix + record.toString());
		}
		else {
			this.stream.println(record.toString());
		}
	}

	@Override
	public void close() {
		this.stream = null;
		this.prefix = null;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "Print to " + (target == STD_OUT ? "System.out" : "System.err");
	}
}
