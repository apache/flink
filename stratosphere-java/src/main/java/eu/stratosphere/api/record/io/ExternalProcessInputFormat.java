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

package eu.stratosphere.api.record.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.StringTokenizer;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.GenericInputSplit;

/**
 * This input format starts an external process and reads its input from the standard out (stdout) of the started process.
 * The process is started outside of the JVM via a provided start command and can be an arbitrary program, e.g., a data generator or a shell script.
 * The input format checks the exit code of the process to validate whether the process terminated correctly. A list of allowed exit codes can be provided.
 * The input format requires ({@link ExternalProcessInputSplit} objects that hold the command to execute.
 * 
 * <b> Attention! </b><br>  
 * You must take care to read from (and process) both output streams of the process, standard out (stdout) and standard error (stderr). 
 * Otherwise, the input format might get deadlocked! 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <T>, The type of the input split (must extend ExternalProcessInputSplit)
 */
public abstract class ExternalProcessInputFormat<T extends ExternalProcessInputSplit> extends GenericInputFormat {
	private static final long serialVersionUID = 1L;
	 
	/**
	 * The config parameter lists (comma separated) all allowed exit codes
	 */
	public static final String ALLOWEDEXITCODES_PARAMETER_KEY = "pact.input.externalProcess.allowedExitCodes";
	
	/**
	 * The external process
	 */
	private Process extProc;
	
	/**
	 * The stdout stream of the external process
	 */
	protected InputStream extProcOutStream;
	
	/**
	 * The stderr stream of the external process
	 */
	protected InputStream extProcErrStream;
	
	/**
	 * Array of allowed exit codes
	 */
	protected int[] allowedExitCodes;
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.api.record.io.input.InputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{

		// get allowed exit codes
		String allowedExitCodesList = parameters.getString(ALLOWEDEXITCODES_PARAMETER_KEY, "0");
		
		// parse allowed exit codes
		StringTokenizer st = new StringTokenizer(allowedExitCodesList, ",");
		this.allowedExitCodes = new int[st.countTokens()];
		
		for(int i=0; i<this.allowedExitCodes.length; i++) {
			this.allowedExitCodes[i] = Integer.parseInt(st.nextToken().trim());
		}
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.api.record.io.InputFormat#close()
	 */
	@Override
	public void close() throws IOException {
		
		try {
			// get exit code
			int exitCode = this.extProc.exitValue();
			// check whether exit code is allowed
			boolean exitCodeOk = false;
			for(int i=0; i<this.allowedExitCodes.length; i++) {
				if(this.allowedExitCodes[i] == exitCode) {
					exitCodeOk = true;
					break;
				}
			}
			if(!exitCodeOk) {
				// external process did not finish with an allowed exit code
				throw new RuntimeException("External process did not finish with an allowed exit code: "+exitCode);
			}
		} catch(IllegalThreadStateException itse) {
			// process did not terminate yet, shut it down!
			this.extProc.destroy();
			if(!this.reachedEnd()) {
				throw new RuntimeException("External process was destroyed although stream was not fully read.");
			}
		} finally {
			this.extProcErrStream.close();
			this.extProcOutStream.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.api.record.io.input.InputFormat#open(eu.stratosphere.nephele.template.InputSplit)
	 */
	@Override
	public void open(GenericInputSplit split) throws IOException {
		
		if(!(split instanceof ExternalProcessInputSplit)) {
			throw new IOException("Invalid InputSplit type.");
		}
		
		ExternalProcessInputSplit epSplit = (ExternalProcessInputSplit)split;		
		
		// check if process command is valid string
		if(epSplit.getExternalProcessCommand() != null && !epSplit.getExternalProcessCommand().equals("")) {
			try {
				// run the external process
				this.extProc = Runtime.getRuntime().exec(epSplit.getExternalProcessCommand());
			} catch (IOException e) {
				throw new IOException("IO Exception when starting external process: "+epSplit.getExternalProcessCommand());
			}
			// connect streams to stdout and stderr
			this.extProcOutStream = this.extProc.getInputStream();
			this.extProcErrStream = this.extProc.getErrorStream();
		} else {
			throw new IllegalArgumentException("External Process Command not set");
		}
	}
	
	public void waitForProcessToFinish() throws InterruptedException {
		extProc.waitFor();
	}
	
}
