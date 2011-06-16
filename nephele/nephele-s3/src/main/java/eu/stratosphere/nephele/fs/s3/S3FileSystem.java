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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class provides a {@link FileSystem} implementation which is backed by Amazon's Simple Storage Service (S3). The
 * implementation uses the REST API of Amazon S3 to facilitate the communication and read/write the data.
 * 
 * @author warneke
 */
public final class S3FileSystem extends FileSystem {

	/**
	 * The logging object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(S3FileSystem.class);

	/**
	 * The configuration key to access the S3 host.
	 */
	public static final String S3_HOST_KEY = "fs.s3.host";

	/**
	 * The configuration key to access the S3 port.
	 */
	public static final String S3_PORT_KEY = "fs.s3.port";

	/**
	 * The configuration key to access the S3 access key.
	 */
	public static final String S3_ACCESS_KEY_KEY = "fs.s3.accessKey";

	/**
	 * The configuration key to access the S3 secret key.
	 */
	public static final String S3_SECRET_KEY_KEY = "fs.s3.secretKey";

	/**
	 * The default host to connect to.
	 */
	private static final String DEFAULT_S3_HOST = "s3.amazonaws.com";

	/**
	 * The default port to connect to.
	 */
	private static final int DEFAULT_S3_PORT = 80;

	/**
	 * The prefix of the HTTP protocol.
	 */
	private static final String HTTP_PREFIX = "http";

	/**
	 * The error code for "resource not found" according to the HTTP protocol.
	 */
	private static final int HTTP_RESOURCE_NOT_FOUND_CODE = 404;
	
	/**
	 * The character which S3 uses internally to indicate an object represents a directory.
	 */
	private static final char S3_DIRECTORY_SEPARATOR = '/';

	/**
	 * The host to address the REST requests to.
	 */
	private String host = null;

	private int port = -1;

	private AmazonS3Client s3Client = null;

	private S3DirectoryStructure directoryStructure = null;

	@Override
	public Path getWorkingDirectory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URI getUri() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(URI name) throws IOException {

		this.host = name.getHost();
		if (this.host == null) {
			LOG.debug("Provided URI does not provide a host to connect to, using configuration...");
			this.host = GlobalConfiguration.getString(S3_HOST_KEY, DEFAULT_S3_HOST);
		}

		this.port = name.getPort();
		if (this.port == -1) {
			LOG.debug("Provided URI does not provide a port to connect to, using configuration...");
			this.port = GlobalConfiguration.getInteger(S3_PORT_KEY, DEFAULT_S3_PORT);
		}

		final String userInfo = name.getUserInfo();

		String awsAccessKey = null;
		String awsSecretKey = null;

		if (userInfo != null) {

			final String[] splits = userInfo.split(":");
			if (splits.length > 1) {
				awsAccessKey = splits[0];
				awsSecretKey = splits[1];
			}
		}

		if (awsAccessKey == null) {
			LOG.debug("Provided URI does not provide an access key to Amazon S3, using configuration...");
			awsAccessKey = GlobalConfiguration.getString(S3_ACCESS_KEY_KEY, null);
			if (awsAccessKey == null) {
				throw new IOException("Cannot determine access key to Amazon S3");
			}
		}

		if (awsSecretKey == null) {
			LOG.debug("Provided URI does not provide a secret key to Amazon S3, using configuration...");
			awsSecretKey = GlobalConfiguration.getString(S3_SECRET_KEY_KEY, null);
			if (awsSecretKey == null) {
				throw new IOException("Cannot determine secret key to Amazon S3");
			}
		}

		final AWSCredentials credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
		this.s3Client = new AmazonS3Client(credentials);

		initializeDirectoryStructure(name);
	}

	private void initializeDirectoryStructure(final URI name) throws IOException {

		String basePath = name.getPath();
		while (true) {

			try {
				final String endpoint = new URL(HTTP_PREFIX, this.host, this.port, basePath).toString();
				if (LOG.isDebugEnabled()) {
					LOG.debug("Trying S3 endpoint " + endpoint);
				}

				this.s3Client.setEndpoint(endpoint);
				final Owner owner = this.s3Client.getS3AccountOwner();
				LOG.info("Successfully established connection to Amazon S3 using the endpoint " + endpoint);
				LOG.info("Amazon S3 user is " + owner.getDisplayName());

				break;
			} catch (MalformedURLException e) {
				throw new IOException(StringUtils.stringifyException(e));
			} catch (AmazonClientException e) {

				// Truncate path
				if (basePath.isEmpty()) {
					throw new IOException("Cannot establish connection to Amazon S3: "
						+ StringUtils.stringifyException(e));
				} else {
					final int pos = basePath.lastIndexOf(Path.SEPARATOR);
					if (pos < 0) {
						basePath = "";
					} else {
						basePath = basePath.substring(0, pos);
					}
				}
			}
		}

		// Finally, create directory structure object
		this.directoryStructure = new S3DirectoryStructure(basePath);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);

		// This is the S3:/// base directory
		if (!bop.hasBucket() && !bop.hasObject()) {

			return new S3FileStatus(f, 0L, true);
		}

		try {
			if (bop.hasBucket() && !bop.hasObject()) {

				// Check if the bucket really exists
				if (!this.s3Client.doesBucketExist(bop.getBucket())) {
					throw new FileNotFoundException("Cannot find " + f.toUri());
				}

				return new S3FileStatus(f, 0L, true);
			}

			try {
				final ObjectMetadata om = this.s3Client.getObjectMetadata(bop.getBucket(), bop.getObject());
				if (objectRepresentsDirectory(bop.getObject(), om.getContentLength())) {
					return new S3FileStatus(f, 0L, true);
				} else {
					return new S3FileStatus(f, om.getContentLength(), false);
				}

			} catch (AmazonServiceException e) {
				if (e.getStatusCode() == HTTP_RESOURCE_NOT_FOUND_CODE) {
					throw new FileNotFoundException("Cannot find " + f.toUri());
				} else {
					throw e;
				}
			}
		} catch (AmazonClientException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FSDataInputStream open(Path f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus[] listStatus(Path f) throws IOException {

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);

		try {

			if (!bop.hasBucket()) {

				final List<Bucket> list = this.s3Client.listBuckets();
				final S3FileStatus[] array = new S3FileStatus[list.size()];
				final Iterator<Bucket> it = list.iterator();
				int i = 0;
				while (it.hasNext()) {
					final Bucket bucket = it.next();
					final S3FileStatus status = new S3FileStatus(extendPath(f, bucket.getName()), 0, true);
					array[i++] = status;
				}

				return array;
			}

			if (bop.hasBucket() && !bop.hasObject()) {

				ObjectListing listing = null;
				final List<S3FileStatus> resultList = new ArrayList<S3FileStatus>();

				// Check if the bucket really exists
				if (!this.s3Client.doesBucketExist(bop.getBucket())) {
					throw new FileNotFoundException("Cannot find " + f.toUri());
				}

				while (true) {

					if (listing == null) {
						listing = this.s3Client.listObjects(bop.getBucket());
					} else {
						listing = this.s3Client.listNextBatchOfObjects(listing);
					}

					final List<S3ObjectSummary> list = listing.getObjectSummaries();
					final Iterator<S3ObjectSummary> it = list.iterator();
					while (it.hasNext()) {

						final S3ObjectSummary os = it.next();
						if (objectRepresentsDirectory(os)) {
							resultList.add(new S3FileStatus(extendPath(f, os.getKey()), 0, true));
						} else {
							resultList.add(new S3FileStatus(extendPath(f, os.getKey()), os.getSize(), false));
						}
					}

					if (!listing.isTruncated()) {
						break;
					}
				}

				return resultList.toArray(new FileStatus[0]);

			} else {

				final ObjectMetadata omd = this.s3Client.getObjectMetadata(bop.getBucket(), bop.getObject());

			}

		} catch (AmazonClientException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {

		try {
			final FileStatus fileStatus = getFileStatus(f); // Will throw a FileNotFoundException if f is invalid
			final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);

			if (fileStatus.isDir()) {

				boolean retVal = false;
				final FileStatus[] dirContent = listStatus(f);
				if (dirContent.length > 0) {
					// Directory is not empty
					if (!recursive) {
						throw new IOException("Found non-empty directory " + f
							+ " while performing non-recursive delete");
					}

					for (FileStatus entry : dirContent) {

						if (delete(entry.getPath(), true)) {
							retVal = true;
						}
					}
				}

				// Now the directory is empty

				if (!bop.hasBucket()) {
					// This is the root directory, do not delete this
					return retVal;
				}

				if (!bop.hasObject()) {
					// This is a real bucket
					this.s3Client.deleteBucket(bop.getBucket());
				} else {
					// This directory is actually represented by an object in S3
					this.s3Client.deleteObject(bop.getBucket(), bop.getObject());
				}
			} else {
				// This is a file
				this.s3Client.deleteObject(bop.getBucket(), bop.getObject());
			}
		} catch (AmazonClientException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean mkdirs(final Path f) throws IOException {

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);
		if (!bop.hasBucket() && !bop.hasObject()) {
			// Ignore this call
			return false;
		}

		boolean retCode = false;

		try {

			// Make sure the bucket exists
			if (bop.hasBucket()) {
				if (this.s3Client.doesBucketExist(bop.getBucket())) {
				} else {
					this.s3Client.createBucket(bop.getBucket());
					retCode = true;
				}
			}

			if (bop.hasObject()) {

				// Make sure object name ends with a directory separator character
				String object = bop.getObject();
				if (!object.isEmpty()) {
					if (object.charAt(object.length() - 1) != S3_DIRECTORY_SEPARATOR) {
						object = object.concat(Character.toString(S3_DIRECTORY_SEPARATOR));
					}
				}

				try {
					this.s3Client.getObjectMetadata(bop.getBucket(), object);
				} catch (AmazonServiceException e) {
					if (e.getStatusCode() == HTTP_RESOURCE_NOT_FOUND_CODE) {
						createEmptyObject(bop.getBucket(), object);
					} else {
						// Rethrow the exception
						throw e;
					}
				}
			}
		} catch (AmazonClientException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}

		return retCode;
	}

	private void createEmptyObject(final String bucketName, final String objectName) {

		final InputStream im = new InputStream() {

			@Override
			public int read() throws IOException {

				return -1;
			}
		};

		final ObjectMetadata om = new ObjectMetadata();
		om.setContentLength(0L);

		this.s3Client.putObject(bucketName, objectName, im, om);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite, final int bufferSize, final short replication, final long blockSize)
			throws IOException {

		if (!overwrite && exists(f)) {
			throw new IOException(f.toUri() + " already exists");
		}

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);
		if(!bop.hasBucket() || !bop.hasObject()) {
			throw new IOException(f.toUri() + " is not a valid path to create a new file");
		}
		
		if(bufferSize < S3DataOutputStream.MINIMUM_MULTIPART_SIZE) {
			throw new IOException("Provided buffer must be at least " + S3DataOutputStream.MINIMUM_MULTIPART_SIZE + " bytes");
		}
		
		final byte[] buf = new byte[bufferSize]; //TODO: Use memory manager to allocate larger pages
		
		return new S3DataOutputStream(this.s3Client, bop.getBucket(), bop.getObject(), buf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite) throws IOException {

		return create(f, overwrite, S3DataOutputStream.MINIMUM_MULTIPART_SIZE, (short) 1, 1024L);
	}

	/*static String getElementFromPath(final Path path, final int pos) {

		if (pos >= path.depth() || pos < 0) {
			return null;
		}

		final String p = path.toUri().getPath();
		int count = 0;
		int startPos = 1;
		int endPos = -1;
		while (count <= pos) {

			if (endPos > 0) {
				startPos = endPos + 1;
			}

			endPos = p.indexOf(Path.SEPARATOR, startPos);
			if (endPos < 0) {
				break;
			}

			++count;
		}

		if (endPos < 0) {
			endPos = p.length();
		}

		return p.substring(startPos, endPos);
	}*/

	private boolean objectRepresentsDirectory(final S3ObjectSummary os) {

		return objectRepresentsDirectory(os.getKey(), os.getSize());
	}

	private boolean objectRepresentsDirectory(final String name, final long size) {

		if (name.isEmpty()) {
			return false;
		}

		if (name.charAt(name.length() - 1) == S3_DIRECTORY_SEPARATOR && size == 0L) {
			return true;
		}

		return false;
	}

	static Path extendPath(final Path parent, final String extension) throws IOException {

		final URI parentUri = parent.toUri();

		if (extension.isEmpty()) {
			return parent;
		}

		final String path = parentUri.getPath();
		String extendedPath;
		if (path.isEmpty()) {
			if (extension.charAt(0) == Path.SEPARATOR_CHAR) {
				extendedPath = extension;
			} else {
				extendedPath = Path.SEPARATOR + extension;
			}
		} else {
			if (path.charAt(path.length() - 1) == Path.SEPARATOR_CHAR) {
				if (extension.charAt(0) == Path.SEPARATOR_CHAR) {
					if (extension.length() > 1) {
						extendedPath = path + extension.substring(1);
					} else {
						extendedPath = path;
					}
				} else {
					extendedPath = path + extension;
				}
			} else {
				if (extension.charAt(0) == Path.SEPARATOR_CHAR) {
					extendedPath = path + extension;
				} else {
					extendedPath = path + Path.SEPARATOR + extension;
				}
			}
		}

		try {
			final URI extendedUri = new URI(parentUri.getScheme(),
				((parentUri.getAuthority() != null) ? parentUri.getAuthority() : ""), extendedPath,
				parentUri.getQuery(), parentUri.getFragment());
			return new Path(extendedUri);
		} catch (URISyntaxException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}
}
