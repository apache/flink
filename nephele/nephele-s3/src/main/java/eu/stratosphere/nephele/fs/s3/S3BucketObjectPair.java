/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

/**
 * An S3 bucket-object pair identifies either a bucket in S3 or an object. If the object property is <code>null</code>,
 * this object identifies an S3 bucket. If both the bucket and the object property is <code>null</code>, the object
 * refers to the S3 base directory.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class S3BucketObjectPair {

	/**
	 * The name of the S3 bucket this object refers to.
	 */
	private final String bucket;

	/**
	 * The name of the S3 object this object refers to.
	 */
	private final String object;

	/**
	 * Creates a new S3 bucket-object pair.
	 * 
	 * @param bucket
	 *        the name of the S3 bucket this object refers to
	 * @param object
	 *        the name of the S3 object this object refers to
	 */
	S3BucketObjectPair(final String bucket, final String object) {
		this.bucket = bucket;
		this.object = object;
	}

	/**
	 * Returns the name of the S3 bucket this object refers to.
	 * 
	 * @return the name of the S3 bucket this object refers to or <code>null</code> if this object refers to the S3 base
	 *         directory
	 */
	public String getBucket() {
		return this.bucket;
	}

	/**
	 * Returns the name of the S3 object this object refers to.
	 * 
	 * @return the name of the S3 object this object refers to or <code>null</code> if this object refers to an S3
	 *         bucket
	 */
	public String getObject() {
		return this.object;
	}

	/**
	 * Checks whether this object refers to an S3 bucket.
	 * 
	 * @return <code>true</code> if this object refers to an S3 bucket, <code>false</code> otherwise
	 *         directory
	 */
	public boolean hasBucket() {
		return (this.bucket != null);
	}

	/**
	 * Checks whether this object refers to an S3 object.
	 * 
	 * @return <code>true</code> if this object refers to an S3 object, <code>false</code> otherwise
	 */
	public boolean hasObject() {
		return (this.object != null);
	}
}
