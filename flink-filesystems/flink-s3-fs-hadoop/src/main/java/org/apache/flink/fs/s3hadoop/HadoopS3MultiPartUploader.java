/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.fs.s3.common.writer.S3MultiPartUploader;
import org.apache.flink.util.MathUtils;

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link S3MultiPartUploader} for the Hadoop S3A filesystem.
 */
public class HadoopS3MultiPartUploader implements S3MultiPartUploader {

	private final S3AFileSystem s3a;

	private final InternalWriteOperationHelper s3uploader;

	public HadoopS3MultiPartUploader(S3AFileSystem s3a, Configuration conf) {
		this.s3uploader = new InternalWriteOperationHelper(
				checkNotNull(s3a),
				checkNotNull(conf)
		);
		this.s3a = s3a;
	}

	@Override
	public String startMultiPartUpload(String key) throws IOException {
		return s3uploader.initiateMultiPartUpload(key);
	}

	@Override
	public UploadPartResult uploadPart(String key, String uploadId, int partNumber, InputStream inputStream, long length) throws IOException {
		final UploadPartRequest uploadRequest = s3uploader.newUploadPartRequest(
				key, uploadId, partNumber, MathUtils.checkedDownCast(length), inputStream, null, 0L);
		return s3uploader.uploadPart(uploadRequest);
	}

	@Override
	public PutObjectResult uploadIncompletePart(String key, InputStream inputStream, long length) throws IOException {
		final PutObjectRequest putRequest = s3uploader.createPutObjectRequest(key, inputStream, length);
		return s3uploader.putObject(putRequest);
	}

	@Override
	public CompleteMultipartUploadResult commitMultiPartUpload(String destKey, String uploadId, List<PartETag> partETags, long length, AtomicInteger errorCount) throws IOException {
		return s3uploader.completeMPUwithRetries(destKey, uploadId, partETags, length, errorCount);
	}

	@Override
	public ObjectMetadata getObjectMetadata(String key) throws IOException {
		try {
			return s3a.getObjectMetadata(new Path('/' + key));
		}
		catch (SdkBaseException e) {
			throw S3AUtils.translateException("getObjectMetadata", key, e);
		}
	}

	/**
	 * Internal {@link WriteOperationHelper} that is wrapped so that it only exposes
	 * the functionality we need for the {@link S3MultiPartUploader}.
	 */
	private static final class InternalWriteOperationHelper extends WriteOperationHelper {

		InternalWriteOperationHelper(S3AFileSystem owner, Configuration conf) {
			super(owner, conf);
		}
	}
}
