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

package eu.stratosphere.runtime.fs.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Date;
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

import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.BlockLocation;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.StringUtils;

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
	 * The configuration key to access the S3 Reduced Redundancy Storage setting.
	 */
	public static final String S3_RRS_KEY = "fs.s3.rrs";

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
	 * The default setting whether to use S3 Reduced Redundancy Storage
	 */
	private static final boolean DEFAULT_S3_RRS = true;

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
	 * The scheme which is used by this file system.
	 */
	public static final String S3_SCHEME = "s3";

	/**
	 * The character set with which the URL is expected to be encoded
	 */
	private static final String URL_ENCODE_CHARACTER = "UTF-8";

	/**
	 * The host to address the REST requests to.
	 */
	private String host = null;

	private int port = -1;

	private URI s3Uri = null;

	private AmazonS3Client s3Client = null;

	private S3DirectoryStructure directoryStructure = null;

	private final boolean useRRS;

	public S3FileSystem() {

		this.useRRS = GlobalConfiguration.getBoolean(S3_RRS_KEY, DEFAULT_S3_RRS);
		LOG.info("Creating new S3 file system binding with Reduced Redundancy Storage "
			+ (this.useRRS ? "enabled" : "disabled"));
	}


	@Override
	public Path getWorkingDirectory() {

		return new Path(this.s3Uri);
	}


	@Override
	public URI getUri() {

		return this.s3Uri;
	}


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
				awsAccessKey = URLDecoder.decode(splits[0], URL_ENCODE_CHARACTER);
				awsSecretKey = URLDecoder.decode(splits[1], URL_ENCODE_CHARACTER);
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

		// Set the S3 URI
		try {
			this.s3Uri = new URI(S3_SCHEME, (String) null, this.host, this.port, basePath, null, null);
		} catch (URISyntaxException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}

		// Finally, create directory structure object
		this.directoryStructure = new S3DirectoryStructure(basePath);
	}


	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);

		// This is the S3:/// base directory
		if (!bop.hasBucket() && !bop.hasObject()) {
			return new S3FileStatus(f, 0L, true, 0L, 0L);
		}

		try {
			if (bop.hasBucket() && !bop.hasObject()) {

				final List<Bucket> buckets = this.s3Client.listBuckets();
				final Iterator<Bucket> it = buckets.iterator();

				// Iterator throw list of buckets to find out creation date
				while (it.hasNext()) {

					final Bucket bucket = it.next();
					if (bop.getBucket().equals(bucket.getName())) {

						final long creationDate = dateToLong(bucket.getCreationDate());
						// S3 does not track access times, so this implementation always sets it to 0
						return new S3FileStatus(f, 0L, true, creationDate, 0L);
					}
				}

				throw new FileNotFoundException("Cannot find " + f.toUri());
			}

			try {
				final ObjectMetadata om = this.s3Client.getObjectMetadata(bop.getBucket(), bop.getObject());
				final long modificationDate = dateToLong(om.getLastModified());
				// S3 does not track access times, so this implementation always sets it to 0
				if (objectRepresentsDirectory(bop.getObject(), om.getContentLength())) {
					return new S3FileStatus(f, 0L, true, modificationDate, 0L);
				} else {
					return new S3FileStatus(f, om.getContentLength(), false, modificationDate, 0L);
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

	private static long dateToLong(final Date date) {

		if (date == null) {
			return 0L;
		}

		return date.getTime();
	}


	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file, final long start, final long len)
			throws IOException {

		if ((start + len) > file.getLen()) {
			return null;
		}

		final S3BlockLocation bl = new S3BlockLocation(this.host, file.getLen());

		return new BlockLocation[] { bl };
	}


	@Override
	public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {

		return open(f); // Ignore bufferSize
	}


	@Override
	public FSDataInputStream open(final Path f) throws IOException {

		final FileStatus fileStatus = getFileStatus(f); // Will throw FileNotFoundException if f does not exist

		// Make sure f is not a directory
		if (fileStatus.isDir()) {
			throw new IOException("Cannot open " + f.toUri() + " because it is a directory");
		}

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);
		if (!bop.hasBucket() || !bop.hasObject()) {
			throw new IOException(f.toUri() + " cannot be opened");
		}

		return new S3DataInputStream(this.s3Client, bop.getBucket(), bop.getObject());
	}


	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);

		try {

			if (!bop.hasBucket()) {

				final List<Bucket> list = this.s3Client.listBuckets();
				final S3FileStatus[] array = new S3FileStatus[list.size()];
				final Iterator<Bucket> it = list.iterator();
				int i = 0;
				while (it.hasNext()) {
					final Bucket bucket = it.next();
					final long creationDate = dateToLong(bucket.getCreationDate());
					// S3 does not track access times, so this implementation always sets it to 0
					final S3FileStatus status = new S3FileStatus(extendPath(f, bucket.getName()
						+ S3_DIRECTORY_SEPARATOR), 0, true, creationDate, 0L);
					array[i++] = status;
				}

				return array;
			}

			if (bop.hasBucket() && !bop.hasObject()) {

				// Check if the bucket really exists
				if (!this.s3Client.doesBucketExist(bop.getBucket())) {
					throw new FileNotFoundException("Cannot find " + f.toUri());
				}

				return listBucketContent(f, bop);

			} else {

				final ObjectMetadata omd = this.s3Client.getObjectMetadata(bop.getBucket(), bop.getObject());
				if (objectRepresentsDirectory(bop.getObject(), omd.getContentLength())) {

					return listBucketContent(f, bop);

				} else {
					final S3FileStatus fileStatus = new S3FileStatus(f, omd.getContentLength(), false,
						dateToLong(omd.getLastModified()), 0L);

					return new FileStatus[] { fileStatus };
				}

			}

		} catch (AmazonClientException e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	private S3FileStatus[] listBucketContent(final Path f, final S3BucketObjectPair bop) throws IOException {

		ObjectListing listing = null;
		final List<S3FileStatus> resultList = new ArrayList<S3FileStatus>();

		final int depth = (bop.hasObject() ? getDepth(bop.getObject()) + 1 : 0);

		while (true) {

			if (listing == null) {
				if (bop.hasObject()) {
					listing = this.s3Client.listObjects(bop.getBucket(), bop.getObject());
				} else {
					listing = this.s3Client.listObjects(bop.getBucket());
				}
			} else {
				listing = this.s3Client.listNextBatchOfObjects(listing);
			}

			final List<S3ObjectSummary> list = listing.getObjectSummaries();
			final Iterator<S3ObjectSummary> it = list.iterator();
			while (it.hasNext()) {

				final S3ObjectSummary os = it.next();
				String key = os.getKey();

				final int childDepth = getDepth(os.getKey());

				if (childDepth != depth) {
					continue;
				}

				// Remove the prefix
				if (bop.hasObject()) {
					if (key.startsWith(bop.getObject())) {
						key = key.substring(bop.getObject().length());
					}

					// This has been the prefix itself
					if (key.isEmpty()) {
						continue;
					}
				}

				final long modificationDate = dateToLong(os.getLastModified());

				S3FileStatus fileStatus;
				if (objectRepresentsDirectory(os)) {
					fileStatus = new S3FileStatus(extendPath(f, key), 0, true, modificationDate, 0L);
				} else {
					fileStatus = new S3FileStatus(extendPath(f, key), os.getSize(), false, modificationDate, 0L);
				}

				resultList.add(fileStatus);
			}

			if (!listing.isTruncated()) {
				break;
			}
		}

		/*
		 * System.out.println("---- RETURN CONTENT ----");
		 * for (final FileStatus entry : resultList) {
		 * System.out.println(entry.getPath());
		 * }
		 * System.out.println("------------------------");
		 */

		return resultList.toArray(new S3FileStatus[0]);

	}

	private static int getDepth(final String key) {

		int depth = 0;
		int nextStartPos = 0;

		final int length = key.length();

		while (nextStartPos < length) {

			final int sepPos = key.indexOf(S3_DIRECTORY_SEPARATOR, nextStartPos);
			if (sepPos < 0) {
				break;
			} else {
				++depth;
				nextStartPos = sepPos + 1;
			}
		}

		if (length > 0) {
			if (key.charAt(length - 1) == S3_DIRECTORY_SEPARATOR) {
				--depth;
			}
		}

		return depth;
	}


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

					for (final FileStatus entry : dirContent) {

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

				while (true) {

					try {
						this.s3Client.getObjectMetadata(bop.getBucket(), object);
					} catch (AmazonServiceException e) {
						if (e.getStatusCode() == HTTP_RESOURCE_NOT_FOUND_CODE) {
							createEmptyObject(bop.getBucket(), object);

							if (object.length() > 1) {
								final int nextPos = object.lastIndexOf(S3_DIRECTORY_SEPARATOR, object.length() - 2);
								if (nextPos >= 0) {
									object = object.substring(0, nextPos + 1);
									continue;
								}
							}

						} else {
							// Rethrow the exception
							throw e;
						}
					}

					// Object already exists, exit
					break;
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


	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite, final int bufferSize,
			final short replication, final long blockSize)
			throws IOException {

		if (!overwrite && exists(f)) {
			throw new IOException(f.toUri() + " already exists");
		}

		final S3BucketObjectPair bop = this.directoryStructure.toBucketObjectPair(f);
		if (!bop.hasBucket() || !bop.hasObject()) {
			throw new IOException(f.toUri() + " is not a valid path to create a new file");
		}

		if (bufferSize < S3DataOutputStream.MINIMUM_MULTIPART_SIZE) {
			throw new IOException("Provided buffer must be at least " + S3DataOutputStream.MINIMUM_MULTIPART_SIZE
				+ " bytes");
		}

		final byte[] buf = new byte[bufferSize]; // TODO: Use memory manager to allocate larger pages

		return new S3DataOutputStream(this.s3Client, bop.getBucket(), bop.getObject(), buf, this.useRRS);
	}


	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite) throws IOException {

		return create(f, overwrite, S3DataOutputStream.MINIMUM_MULTIPART_SIZE, (short) 1, 1024L);
	}

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


	@Override
	public boolean rename(final Path src, final Path dst) throws IOException {

		throw new UnsupportedOperationException("This method is not yet implemented");
	}
}
