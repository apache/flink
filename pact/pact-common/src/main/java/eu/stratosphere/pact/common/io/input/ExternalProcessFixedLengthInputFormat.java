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

package eu.stratosphere.pact.common.io.input;

import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

public abstract class ExternalProcessFixedLengthInputFormat<T extends ExternalProcessInputSplit, K extends Key, V extends Value> extends ExternalProcessInputFormat<T, K, V> {

	/**
	 * The config parameter which defines the fixed length of a record.
	 */
	public static final String RECORDLENGTH_PARAMETER_KEY = "pact.input.recordLength";
	
	/**
	 * The default read buffer size = 1MB.
	 */
	private static final int DEFAULT_TARGET_READ_BUFFER_SIZE = 1024 * 1024;
	
	/**
	 * Buffer to hold a single record
	 */
	private byte[] recordBuffer;
	
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
	
	public abstract boolean readBytes(KeyValuePair<K,V> pair, byte[] pairBytes);
	
	/**
	 * {@inheritDoc}
	 */
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
	public void open(T split) throws IOException {
		
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
		
		// initialize record buffer
		this.recordBuffer = new byte[this.recordLength];
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
	public boolean nextRecord(KeyValuePair<K,V> record) throws IOException {
		
		// check if read buffer must be filled (less than one record contained)
		if(this.readBufferFillPos - this.readBufferReadPos < this.recordLength) {
			// try to fill read buffer
			if(!this.fillReadBuffer()) {
				return false;
			}
		}
		
		// copy record into record buffer
		System.arraycopy(this.readBuffer, this.readBufferReadPos, this.recordBuffer, 0, this.recordBuffer.length);
		// update read buffer read marker
		this.readBufferReadPos += this.recordBuffer.length;
		
		return this.readBytes(record, recordBuffer);
		
	}

	private boolean fillReadBuffer() throws IOException {
		
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
