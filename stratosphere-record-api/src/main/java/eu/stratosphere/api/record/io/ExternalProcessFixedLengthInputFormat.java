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

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.types.Record;

/**
 * This input format starts an external process and reads its input from the standard out (stdout) of the started process.
 * The input is split into fixed-sized segments from which a {@link Record} is generated. 
 * The external process is started outside of the JVM via a provided start command and can be an arbitrary program, 
 * e.g., a data generator or a shell script. The input format checks the exit code of the process 
 * to validate whether the process terminated correctly. A list of allowed exit codes can be provided.
 * The input format requires ({@link ExternalProcessInputSplit} objects that hold the command to execute.
 * 
 * <b>Warning:</b> This format does not consume the standard error stream (stderr) of the started process. This might cause deadlocks. 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <T>, The type of the input split (must extend ExternalProcessInputSplit)
 * 
 */
public abstract class ExternalProcessFixedLengthInputFormat<T extends ExternalProcessInputSplit> extends ExternalProcessInputFormat<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * The config parameter which defines the fixed length of a record.
	 */
	public static final String RECORDLENGTH_PARAMETER_KEY = "pact.input.recordLength";
	
	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int DEFAULT_TARGET_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * Buffer to read a batch of records from a file 
	 */
	private byte[] readBuffer;
	
	/**
	 * read position within the read buffer
	 */
	private int readBufferReadPos;
	
	/**
	 * fill marker within the read buffer 
	 */
	private int readBufferFillPos;
	
	/**
	 * remaining space within the read buffer
	 */
	private int readBufferRemainSpace;
	
	/**
	 * target size of the read buffer
	 */
	private int targetReadBufferSize = DEFAULT_TARGET_READ_BUFFER_SIZE;
	
	/**
	 * fixed length of all records
	 */
	protected int recordLength;
	
	/**
	 * Flags to indicate the end of the split
	 */
	private boolean noMoreStreamInput;
	private boolean noMoreRecordBuffers;
	
	/**
	 * Reads a record out of the given buffer. This operation always consumes the standard number of
	 * bytes, regardless of whether the produced record was valid.
	 * 
	 * @param target The target Record
	 * @param buffer The buffer containing the binary data.
	 * @param startPos The start position in the byte array.
	 * @return True, is the record is valid, false otherwise.
	 */
	public abstract boolean readBytes(Record target, byte[] buffer, int startPos);
	

	@Override
	public void configure(Configuration parameters)
	{
		// configure parent
		super.configure(parameters);
		
		// read own parameters
		this.recordLength = parameters.getInteger(RECORDLENGTH_PARAMETER_KEY, 0);
		if (recordLength < 1) {
			throw new IllegalArgumentException("The record length parameter must be set and larger than 0.");
		}
		
	}
	
	/**
	 * Sets the target size of the buffer to be used to read from the stdout stream. 
	 * The actual size depends on the record length since it is chosen such that records are not split.
	 * This method has only an effect, if it is called before the input format is opened.
	 * 
	 * @param targetReadBufferSize The target size of the read buffer.
	 */
	public void setTargetReadBufferSize(int targetReadBufferSize)
	{
		this.targetReadBufferSize = targetReadBufferSize;
	}
	

	@Override
	public void open(GenericInputSplit split) throws IOException {
		
		super.open(split);
		
		// compute readBufferSize
		if(recordLength > this.targetReadBufferSize) {
			// read buffer is at least as big as record
			this.readBuffer = new byte[recordLength];
		} else if (this.targetReadBufferSize % recordLength == 0) {
			// target read buffer size is a multiple of record length, so it's ok
			this.readBuffer = new byte[this.targetReadBufferSize];
		} else {
			// extent default read buffer size such that records are not split
			this.readBuffer = new byte[(recordLength - (this.targetReadBufferSize % recordLength)) + this.targetReadBufferSize];
		}
		
		// initialize read buffer positions
		this.readBufferReadPos = 0;
		this.readBufferFillPos = 0;
		this.readBufferRemainSpace = readBuffer.length;
		// initialize end flags
		this.noMoreStreamInput = false;
		this.noMoreRecordBuffers = false;
		
	}
	

	@Override
	public boolean reachedEnd() throws IOException {
		return noMoreRecordBuffers;
	}
	

	@Override
	public boolean nextRecord(Record record) throws IOException {
		
		// check if read buffer must be filled (less than one record contained)
		if(this.readBufferFillPos - this.readBufferReadPos < this.recordLength) {
			// try to fill read buffer
			if(!this.fillReadBuffer()) {
				return false;
			}
		}

		// update read buffer read marker
		this.readBufferReadPos += this.recordLength;
		
		return this.readBytes(record, readBuffer, (this.readBufferReadPos-this.recordLength));
		
	}

	/**
	 * Fills the read buffer by reading from the stdout stream of the external process.
	 * WARNING: We do not read from the error stream. This might cause a deadlock.
	 *  
	 * @return true if new content was filled into the buffer, false otherwise.
	 * @throws IOException
	 */
	private boolean fillReadBuffer() throws IOException {
		// TODO: Add reading from error stream of external process. Otherwise the InputFormat might get deadlocked!
		
		// stream was completely processed
		if(noMoreStreamInput) {
			if(this.readBufferReadPos == this.readBufferFillPos) {
				this.noMoreRecordBuffers = true;
				return false;
			} else {
				throw new RuntimeException("External process produced incomplete record");
			}
		}
		
		// the buffer was completely filled and processed
		if(this.readBufferReadPos == this.readBuffer.length && 
				this.readBufferRemainSpace == 0) {
			// reset counters and fill again
			this.readBufferFillPos = 0;
			this.readBufferRemainSpace = this.readBuffer.length;
			this.readBufferReadPos = 0;
		}
		
		// as long as not at least one record is complete
		while(this.readBufferFillPos - this.readBufferReadPos < this.recordLength) {
			// read from stdout
			int readCnt = super.extProcOutStream.read(this.readBuffer, this.readBufferFillPos, this.readBufferRemainSpace);
			
			if(readCnt == -1) {
				// the is nothing more to read
				this.noMoreStreamInput = true;
				return false;
			} else {
				// update fill position and remain cnt
				this.readBufferFillPos += readCnt;
				this.readBufferRemainSpace -= readCnt;
			}
		}
		return true;
	}

}
