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

package eu.stratosphere.pact.common.recordio;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.template.InputSplit;


/**
 * Describes the base interface that is used for reading from a file input.
 * For specific input types the createPair(), nextPair(), reachedEnd()
 * methods need to be implemented. Additionally it is advised to
 * override the open() and close() methods declared in stub to handle
 * input specific settings.
 * While reading the runtime checks whether the end was reached using reachedEnd()
 * and if not the next pair is read using the nextPair() method.
 * 
 * @author Moritz Kaufmann
 * @author Stephan Ewen
 */
public abstract class FileInputFormat implements InputFormat
{
	/**
	 * The LOG for logging messages in this class.
	 */
	public static final Log LOG = LogFactory.getLog(FileInputFormat.class);
	
	/**
	 * The maximal time that the format waits for a split to be opened before declaring failure.
	 */
	public static final long OPEN_TIMEOUT_MILLIES = 10000;
	
	/**
	 * The default read buffer size = 1MB.
	 */
	public static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;
	
	// --------------------------------------------------------------------------------------------
	
	protected FSDataInputStream stream;

	protected long start;

	protected long length;

	protected int bufferSize;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Default implementation of the configure method that does nothing.
	 * 
	 * @see eu.stratosphere.pact.common.recordio.InputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters)
	{}
	
	@Override
	public void open(InputSplit split) throws IOException
	{
		if (!(split instanceof FileInputSplit)) {
			throw new IllegalArgumentException("File Input Formats can only be used with FileInputSplits.");
		}
		
		final FileInputSplit fileSplit = (FileInputSplit) split;
		
		this.start = fileSplit.getStart();
		this.length = fileSplit.getLength();

		if (LOG.isDebugEnabled())
			LOG.debug("Opening input split " + fileSplit.getPath() + " [" + start + "," + length + "]");

		
		// open the split in an asynchronous thread
		final InputSplitOpenThread isot = new InputSplitOpenThread(fileSplit);
		isot.start();
		
		long openStartTime = System.currentTimeMillis();
		long remaining = OPEN_TIMEOUT_MILLIES;
		try {
			do {
				isot.join(remaining);
			}
			while ((remaining = System.currentTimeMillis() - openStartTime) > 0);
			if (isot.getFSDataInputStream() == null) {
				throw new IOException("Opening input split " + fileSplit.getPath() + 
					" [" + start + "," + length + "] timed out.");
			}
		}
		catch (InterruptedException ie) {
			// task has been canceled
			if (isot.getFSDataInputStream() != null) {
				// close file input stream
				isot.getFSDataInputStream().close();
			}
			throw new IOException("Opening the Input Split was interrupted.");
		}
		
		// check if FSDataInputStream was obtained
		if (!isot.fsDataInputStreamSuccessfullyObtained()) {
			// forward exception
			Exception e = isot.getException();
			if (e != null && e instanceof IOException) {
				throw (IOException) e;
			}
			else {
				throw new IOException("Opening input split " + fileSplit.getPath() + 
					" [" + start + "," + length + "] caused an error" + (e != null ? (": " + e.getMessage()) : "."), e);  
			}
		}

		// get FSDataInputStream
		this.stream = isot.getFSDataInputStream();
		this.bufferSize = DEFAULT_READ_BUFFER_SIZE;
	}
	
	/**
	 * Closes the input stream of the input format.
	 */
	@Override
	public void close() throws IOException
	{
		if (this.stream != null) {
			// close input stream
			this.stream.close();
		}
	}
	
	
	
	// ============================================================================================
	
	/**
	 * Obtains a DataInputStream in an thread that is not interrupted.
	 * This is a necessary hack around the problem that the HDFS client is very sensitive to InterruptedExceptions.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	public static class InputSplitOpenThread extends Thread {

		private FileInputSplit split;

		private FSDataInputStream fdis = null;

		private boolean success = true;

		private Exception exception = null;

		public InputSplitOpenThread(FileInputSplit split) {
			this.split = split;
		}

		@Override
		public void run() {
			try {
				FileSystem fs = FileSystem.get(split.getPath().toUri());
				fdis = fs.open(split.getPath());
				
			}
			catch (Exception t) {
				this.success = false;
				this.exception = t;
			}
		}

		public FSDataInputStream getFSDataInputStream() {
			return this.fdis;
		}

		public boolean fsDataInputStreamSuccessfullyObtained() {
			return this.success;
		}

		public Exception getException() {
			return this.exception;
		}
	}
}
