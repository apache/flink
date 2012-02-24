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
package eu.stratosphere.pact.common.io;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * 
 * 
 * @author Arvid Heise
 */
public abstract class BinaryOutputFormat extends FileOutputFormat {
	/**
	 * The config parameter which defines the fixed length of a record.
	 */
	public static final String BLOCK_SIZE_PARAMETER_KEY = "pact.output.block_size";

	public static final long NATIVE_BLOCK_SIZE = Long.MIN_VALUE;

	/**
	 * The block size to use.
	 */
	private long blockSize = NATIVE_BLOCK_SIZE;

	private DataOutputStream dataOutputStream;

	private BlockBasedOutput blockBasedInput;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileOutputFormat#close()
	 */
	@Override
	public void close() throws IOException {
		this.dataOutputStream.close();
		super.close();
	}

	protected void complementBlockInfo(BlockInfo blockInfo) throws IOException {
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.FileInputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);

		// read own parameters
		this.blockSize = parameters.getLong(BLOCK_SIZE_PARAMETER_KEY, NATIVE_BLOCK_SIZE);
		if (this.blockSize < 1 && this.blockSize != NATIVE_BLOCK_SIZE)
			throw new IllegalArgumentException("The block size parameter must be set and larger than 0.");
		if (this.blockSize > Integer.MAX_VALUE)
			throw new UnsupportedOperationException("Currently only block size up to Integer.MAX_VALUE are supported");
	}

	protected BlockInfo createBlockInfo() {
		return new BlockInfo();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.FileOutputFormat#open(int)
	 */
	@Override
	public void open(int taskNumber) throws IOException {
		super.open(taskNumber);

		final long blockSize = this.blockSize == NATIVE_BLOCK_SIZE ?
			this.outputFilePath.getFileSystem().getDefaultBlockSize() : this.blockSize;

		this.blockBasedInput = new BlockBasedOutput(this.stream, (int) blockSize);
		this.dataOutputStream = new DataOutputStream(this.blockBasedInput);
	}

	protected abstract void serialize(PactRecord record, DataOutput dataOutput) throws IOException;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.OutputFormat#writeRecord(eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public void writeRecord(PactRecord record) throws IOException {
		this.blockBasedInput.startRecord();
		this.serialize(record, this.dataOutputStream);
	}

	/**
	 * Writes a block info at the end of the blocks.<br>
	 * Current implementation uses only int and not long.
	 * 
	 * @author Arvid Heise
	 */
	protected class BlockBasedOutput extends FilterOutputStream {

		/**
		 * 
		 */
		private static final int NO_RECORD = -1;

		private final int maxPayloadSize;

		private int blockPos;

		private int blockCount, totalCount;

		private long firstRecordStartPos = NO_RECORD;

		private BlockInfo blockInfo = BinaryOutputFormat.this.createBlockInfo();

		private DataOutputStream headerStream;

		public BlockBasedOutput(OutputStream out, int blockSize) {
			super(out);
			this.headerStream = new DataOutputStream(out);
			this.maxPayloadSize = blockSize - this.blockInfo.getInfoSize();
		}

		/*
		 * (non-Javadoc)
		 * @see java.io.FilterOutputStream#close()
		 */
		@Override
		public void close() throws IOException {
			if (this.blockPos > 0)
				this.writeInfo();
			super.flush();
			super.close();
		}

		public void startRecord() {
			if (this.firstRecordStartPos == NO_RECORD)
				this.firstRecordStartPos = this.blockPos;
			this.blockCount++;
			this.totalCount++;
		}

		// /*
		// * (non-Javadoc)
		// * @see java.io.FilterOutputStream#write(byte[])
		// */
		// @Override
		// public void write(byte[] b) throws IOException {
		// this.write(b, 0, b.length);
		// }
		//
		// /*
		// * (non-Javadoc)
		// * @see java.io.FilterOutputStream#write(byte[], int, int)
		// */
		// @Override
		// public void write(byte[] b, int off, int len) throws IOException {
		//
		// // for (int remainingLength = len, offset = off; remainingLength > 0;) {
		// // int blockLen = Math.min(remainingLength, this.maxPayloadSize - this.blockPos);
		// // super.write(b, offset, blockLen);
		// //
		// //
		// // this.blockPos += blockLen;
		// // if (this.blockPos >= this.maxPayloadSize)
		// // this.writeInfo();
		// // remainingLength -= blockLen;
		// // offset += blockLen;
		// // }
		// }

		/*
		 * (non-Javadoc)
		 * @see java.io.FilterOutputStream#write(int)
		 */
		@Override
		public void write(int b) throws IOException {
			super.write(b);
			if (++this.blockPos >= this.maxPayloadSize)
				this.writeInfo();
		}

		private void writeInfo() throws IOException {
			this.blockInfo.setRecordCount(this.blockCount);
			this.blockInfo.setAccumulatedRecordCount(this.totalCount);
			this.blockInfo.setFirstRecordStart(this.firstRecordStartPos == NO_RECORD ? 0 : this.firstRecordStartPos);
			BinaryOutputFormat.this.complementBlockInfo(this.blockInfo);
			this.blockInfo.write(this.headerStream);
			this.blockPos = 0;
			this.blockCount = 0;
			this.firstRecordStartPos = NO_RECORD;
		}
	}
}
