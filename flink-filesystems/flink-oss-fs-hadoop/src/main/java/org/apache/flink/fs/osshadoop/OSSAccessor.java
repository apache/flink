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

package org.apache.flink.fs.osshadoop;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Core implementation of Aliyun OSS Filesystem for Flink. Provides the bridging logic between
 * Hadoop's abstract filesystem and Aliyun OSS.
 */
public class OSSAccessor {

    private AliyunOSSFileSystem fs;
    private AliyunOSSFileSystemStore store;

    public OSSAccessor(AliyunOSSFileSystem fs) {
        this.fs = fs;
        this.store = fs.getStore();
    }

    public String pathToObject(final Path path) {
        org.apache.hadoop.fs.Path hadoopPath = HadoopFileSystem.toHadoopPath(path);
        if (!hadoopPath.isAbsolute()) {
            hadoopPath = new org.apache.hadoop.fs.Path(fs.getWorkingDirectory(), hadoopPath);
        }

        return hadoopPath.toUri().getPath().substring(1);
    }

    public Path objectToPath(String object) {
        return new Path("/" + object);
    }

    public String startMultipartUpload(String objectName) {
        return store.getUploadId(objectName);
    }

    public boolean deleteObject(String objectName) throws IOException {
        return fs.delete(new org.apache.hadoop.fs.Path('/' + objectName), false);
    }

    public CompleteMultipartUploadResult completeMultipartUpload(
            String objectName, String uploadId, List<PartETag> partETags) {
        return store.completeMultipartUpload(objectName, uploadId, partETags);
    }

    public PartETag uploadPart(File file, String objectName, String uploadId, int idx)
            throws IOException {
        return store.uploadPart(file, objectName, uploadId, idx);
    }

    public void putObject(String objectName, File file) throws IOException {
        store.uploadObject(objectName, file);
    }

    public void getObject(String objectName, String dstPath, long length) throws IOException {
        long contentLength = store.getObjectMetadata(objectName).getContentLength();
        if (contentLength != length) {
            throw new IOException(
                    String.format(
                            "Error recovering writer: "
                                    + "Downloading the last data chunk file gives incorrect length."
                                    + "File length is %d bytes, RecoveryData indicates %d bytes",
                            contentLength, length));
        }

        org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path("/" + objectName);
        org.apache.hadoop.fs.Path localPath = new org.apache.hadoop.fs.Path(dstPath);
        fs.copyToLocalFile(srcPath, localPath);

        String crcFileName = "." + localPath.getName() + ".crc";
        (new File(localPath.getParent().toString() + "/" + crcFileName)).delete();
    }
}
