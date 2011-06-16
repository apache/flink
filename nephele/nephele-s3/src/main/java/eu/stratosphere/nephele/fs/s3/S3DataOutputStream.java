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
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.util.StringUtils;

public final class S3DataOutputStream extends FSDataOutputStream {

	private static final int MAX_PART_NUMBER = 10000;

	public static final int MINIMUM_MULTIPART_SIZE = 5 * 1024 * 1024;

	private final AmazonS3Client s3Client;

	private final byte[] buf;

	private final String bucket;

	private final String object;

	private final List<PartETag> partETags = new ArrayList<PartETag>();

	/**
	 * The ID of a multipart upload in case multipart upload is used, otherwise <code>null</code>.
	 */
	private String uploadId = null;

	/**
	 * The next part number to be used during a multipart upload.
	 */
	private int partNumber = 1; // First valid upload part number is 1.

	private int bytesWritten = 0;

	private final class InternalUploadInputStream extends InputStream {

		private final byte[] srcBuf;

		private final int length;

		private int bytesRead = 0;

		private InternalUploadInputStream(final byte[] srcBuf, final int length) {
			this.srcBuf = buf;
			this.length = length;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int read() throws IOException {

			if (this.length - this.bytesRead == 0) {
				return -1;
			}

			return (int) this.srcBuf[this.bytesRead++];
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int read(final byte[] buf) throws IOException {

			return read(buf, 0, buf.length);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int read(final byte[] buf, final int off, final int len) throws IOException {

			if (this.length - this.bytesRead == 0) {
				return -1;
			}

			final int bytesToCopy = Math.min(len, this.length - this.bytesRead);
			System.arraycopy(srcBuf, this.bytesRead, buf, off, bytesToCopy);
			this.bytesRead += bytesToCopy;

			return bytesToCopy;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int available() throws IOException {

			return (this.length - bytesRead);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public long skip(final long n) throws IOException {

			int bytesToSkip = (int) Math.min(n, Integer.MAX_VALUE);
			bytesToSkip = Math.min(this.length - this.bytesRead, bytesToSkip);

			this.bytesRead += bytesToSkip;

			return bytesToSkip;
		}
	}

	S3DataOutputStream(final AmazonS3Client s3Client, final String bucket, final String object, final byte[] buf) {

		this.s3Client = s3Client;
		this.bucket = bucket;
		this.object = object;
		this.buf = buf;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final int b) throws IOException {

		// Upload buffer to S3
		if (this.bytesWritten == this.buf.length) {
			uploadPartAndFlushBuffer();
		}

		this.buf[this.bytesWritten++] = (byte) b;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {

		int nextPos = off;

		while (nextPos < len) {

			// Upload buffer to S3
			if (this.bytesWritten == this.buf.length) {
				uploadPartAndFlushBuffer();
			}

			final int bytesToCopy = Math.min(this.buf.length - this.bytesWritten, len - nextPos);
			System.arraycopy(b, nextPos, this.buf, this.bytesWritten, bytesToCopy);
			this.bytesWritten += bytesToCopy;
			nextPos += bytesToCopy;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final byte[] b) throws IOException {

		write(b, 0, b.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {

		System.out.println("Finishing upload " + this.bytesWritten);

		if (this.uploadId == null) {
			// This is not a multipart upload

			// No data has been written
			if (this.bytesWritten == 0) {
				return;
			}

			final InputStream is = new InternalUploadInputStream(this.buf, this.bytesWritten);
			final ObjectMetadata om = new ObjectMetadata();
			om.setContentLength(this.bytesWritten);

			try {
				this.s3Client.putObject(this.bucket, this.object, is, om);
			} catch (AmazonServiceException e) {
				throw new IOException(StringUtils.stringifyException(e));
			}

			this.bytesWritten = 0;

		} else {

			if (this.bytesWritten > 0) {
				uploadPartAndFlushBuffer();
			}

			boolean operationSuccessful = false;
			try {
				final CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(this.bucket,
					this.object,
					this.uploadId, this.partETags);
				this.s3Client.completeMultipartUpload(request);

				operationSuccessful = true;

			} catch (AmazonServiceException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} finally {
				if (!operationSuccessful) {
					abortUpload();
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException {

		// Flush does nothing in this implementation since we ways have to transfer at least 5 MB in a multipart upload
	}

	private void uploadPartAndFlushBuffer() throws IOException {

		boolean operationSuccessful = false;

		if (this.uploadId == null) {
			this.uploadId = initiateMultipartUpload();
		}

		try {

			if (this.partNumber >= MAX_PART_NUMBER) {
				throw new IOException("Cannot upload any more data: maximum part number reached");
			}

			System.out.println("Uploading part " + this.partNumber + " with " + this.bytesWritten + " bytes");

			final InputStream inputStream = new InternalUploadInputStream(this.buf, this.bytesWritten);
			final UploadPartRequest request = new UploadPartRequest();
			request.setBucketName(this.bucket);
			request.setKey(this.object);
			request.setInputStream(inputStream);
			request.setUploadId(this.uploadId);
			request.setPartSize(this.bytesWritten);
			request.setPartNumber(this.partNumber++);

			final UploadPartResult result = this.s3Client.uploadPart(request);
			this.partETags.add(result.getPartETag());

			this.bytesWritten = 0;
			operationSuccessful = true;

		} catch (AmazonServiceException e) {
			throw new IOException(StringUtils.stringifyException(e));
		} finally {
			if (!operationSuccessful) {
				abortUpload();
			}
		}
	}

	private String initiateMultipartUpload() throws IOException {

		boolean operationSuccessful = false;
		final InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(this.bucket, this.object);

		try {

			final InitiateMultipartUploadResult result = this.s3Client.initiateMultipartUpload(request);
			operationSuccessful = true;
			return result.getUploadId();

		} catch (AmazonServiceException e) {
			throw new IOException(StringUtils.stringifyException(e));
		} finally {
			if (!operationSuccessful) {
				abortUpload();
			}
		}
	}

	private void abortUpload() {

		if (this.uploadId == null) {
			// This is not a multipart upload, nothing to do here
			return;
		}

		System.out.println("Aborting upload");

		try {
			final AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(this.bucket, this.object,
				this.uploadId);
			this.s3Client.abortMultipartUpload(request);
		} catch (AmazonServiceException e) {
			// Ignore exception
		}
	}
}
