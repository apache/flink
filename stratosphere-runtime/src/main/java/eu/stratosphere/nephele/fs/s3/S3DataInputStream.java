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

package eu.stratosphere.nephele.fs.s3;

import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class implements an {@link FSDataInputStream} that downloads its data from Amazon S3 in the background.
 * Essentially, this class is just a wrapper to the Amazon AWS SDK.
 * 
 * @author warneke
 */
public class S3DataInputStream extends FSDataInputStream {

	/**
	 * The input stream which reads the actual S3 object content.
	 */
	private final InputStream inputStream;

	/**
	 * Constructs a new input stream which reads its data from the specified S3 object.
	 * 
	 * @param s3Client
	 *        the S3 client to connect to Amazon S3.
	 * @param bucket
	 *        the name of the S3 bucket the object is stored in
	 * @param object
	 *        the name of the S3 object whose content shall be read
	 * @throws IOException
	 *         thrown if an error occurs while accessing the specified S3 object
	 */
	S3DataInputStream(final AmazonS3Client s3Client, final String bucket, final String object) throws IOException {

		S3Object s3o = null;
		try {
			s3o = s3Client.getObject(bucket, object);
		} catch (AmazonServiceException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}

		this.inputStream = s3o.getObjectContent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int available() throws IOException {

		return this.inputStream.available();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		this.inputStream.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void mark(final int readlimit) {

		this.inputStream.mark(readlimit);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean markSupported() {

		return this.inputStream.markSupported();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read() throws IOException {

		return this.inputStream.read();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final byte[] b) throws IOException {

		return this.inputStream.read(b);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final byte[] b, final int off, final int len) throws IOException {

		return this.inputStream.read(b, off, len);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() throws IOException {

		this.inputStream.reset();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void seek(final long desired) throws IOException {

		this.inputStream.skip(desired);
	}
}
