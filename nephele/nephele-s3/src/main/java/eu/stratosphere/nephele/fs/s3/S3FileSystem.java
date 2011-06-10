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
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;

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
	private static final String S3_HOST_KEY = "fs.s3.host";

	/**
	 * The configuration key to access the S3 port.
	 */
	private static final String S3_PORT_KEY = "fs.s3.port";

	/**
	 * The configuration key to access the S3 access key.
	 */
	private static final String S3_ACCESS_KEY_KEY = "fs.s3.accessKey";

	/**
	 * The configuration key to access the S3 secret key.
	 */
	private static final String S3_SECRET_KEY_KEY = "fs.s3.secretKey";

	/**
	 * The default host to connect to.
	 */
	private static final String DEFAULT_S3_HOST = "s3.amazonaws.com";

	/**
	 * The default port to connect to.
	 */
	private static final int DEFAULT_S3_PORT = 80;

	/**
	 * The date format string that shall be used to format the date inside of HTTP requests.
	 */
	private final static String DATE_FORMAT_STRING = "EEE, dd MMM yyyy HH:mm:ss ";

	/**
	 * The message authentication code to be used to sign HTTP requests.
	 */
	private final static String MESSAGE_AUTHENTICATION_CODE = "HmacSHA1";

	/**
	 * The HTTP return code for a successful operation.
	 */
	private final static int HTTP_SUCCESS_CODE = 200;

	/**
	 * The HTTP return code for non-existing resources.
	 */
	private final static int HTTP_NOT_FOUND_CODE = 404;

	/**
	 * The HTTP return code for a bad request.
	 */
	private final static int HTTP_BAD_REQUEST = 400;

	/**
	 * The date format object to format the current date.
	 */
	private final SimpleDateFormat dateFormat;

	/**
	 * The encoder object used to convert the authorization signature into base64 format.
	 */
	@SuppressWarnings("restriction")
	private final sun.misc.BASE64Encoder base64Encoder;

	/**
	 * The host to address the REST requsts to.
	 */
	private String host = null;

	private int port = -1;

	private Mac mac;

	private String authorizationPrefix = "AWS WKy3rMzOWPouVOxK1p3Ar1C2uRBwa2FBXnCw:";

	private SecretKeySpec signingKey;

	@SuppressWarnings("restriction")
	public S3FileSystem() {

		// Initialize base64 encoder required to sign requests
		this.base64Encoder = new sun.misc.BASE64Encoder();

		// Initialize the date format object which is required to request HTTP requests
		this.dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING, Locale.US);
		this.dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

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

		System.out.println("Initialization URI: " + name);

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

		LOG.debug("Amazon S3 REST requsts will be sent to host " + this.host + ", port " + this.port);

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

		// Set the authorization prefix
		this.authorizationPrefix = "AWS " + awsAccessKey + ":";

		try {
			this.mac = Mac.getInstance(MESSAGE_AUTHENTICATION_CODE);
			final byte[] keyBytes = awsSecretKey.getBytes("UTF8");
			this.signingKey = new SecretKeySpec(keyBytes, MESSAGE_AUTHENTICATION_CODE);
			this.mac.init(this.signingKey);
		} catch (NoSuchAlgorithmException nsae) {
			throw new IOException(nsae.getMessage());
		} catch (InvalidKeyException ike) {
			throw new IOException(ike.getMessage());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus getFileStatus(Path f) throws IOException {

		final URI uri = f.toUri();
		HttpURLConnection conn = null;
		try {

			conn = openHttpURLConnection("HEAD", uri.getPath());
			final int responseCode = conn.getResponseCode();
			if(responseCode == HTTP_SUCCESS_CODE) {
				// This is an object and we can now retrieve its properties
			} else if(responseCode == HTTP_NOT_FOUND_CODE) {
				throw new FileNotFoundException();
			} else if(responseCode == HTTP_BAD_REQUEST) {
				// Path points to a bucket
				return new S3FileStatus(f, 0, true);
			} else {
				throw new IOException(conn.getResponseMessage());
			}
			
			System.out.println("Response code: " + conn.getResponseCode());

		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		
		

		return null;
	}

	private String sign(String data) throws UnsupportedEncodingException {

		final byte[] signBytes = this.mac.doFinal(data.getBytes("UTF8"));
		return encodeBase64(signBytes);
	}

	private String encodeBase64(final byte[] data) {

		@SuppressWarnings("restriction")
		String base64 = this.base64Encoder.encode(data);
		if (base64.endsWith("\r\n")) {
			base64 = base64.substring(0, base64.length() - 2);
		}

		return base64;
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

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		
		System.out.println("Got here");
		
		final URI uri = f.toUri();
		HttpURLConnection conn = null;
		
		try {
			
			conn = openHttpURLConnection("HEAD", uri.getPath());
			int responseCode = conn.getResponseCode();
			
			if(responseCode == HTTP_BAD_REQUEST) {
				
				//The path points to a bucket
				modifyHttpURLConnection(conn, "GET");
				responseCode = conn.getReadTimeout();
				
				
			} else {
				
				//The path points
			}
			
			System.out.println("Response code: " + responseCode);
			
		} finally {
			if(conn != null) {
				conn.disconnect();
			}
		}
		
		return null;
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean mkdirs(Path f) throws IOException {

		final URI uri = f.toUri();

		HttpURLConnection conn = null;
		try {
			conn = openHttpURLConnection("PUT", uri.getPath());

			if (conn.getResponseCode() != HTTP_SUCCESS_CODE) {
				throw new IOException(conn.getResponseMessage());
			}

		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
			throws IOException {
		
		HttpURLConnection conn = null;
		try {
			conn = openHttpURLConnection("PUT", "/services/Walrus/daniel/test.txt");
			
			
		} finally {
			
			final OutputStream output = conn.getOutputStream();
			for(int i = 0; i < 1024*1024; i++) {
				output.write(0);
			}
			
			output.close();
			
			int responseCode = conn.getResponseCode();
			System.out.println(responseCode + ": " + conn.getResponseMessage());
			
			if(conn != null) {
				conn.disconnect();
			}
		}
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
		
		return create(f, overwrite, 1024, (short) 1, 1024L);
	}

	private String createSignature(final String method, final String contentMD5, final String contentType,
			final String date, final String file) throws UnsupportedEncodingException {

		final StringBuffer buf = new StringBuffer();
		buf.append(method).append("\n");
		buf.append(contentMD5).append("\n");
		buf.append(contentType).append("\n");
		buf.append(date).append("\n");
		buf.append(file);

		return sign(buf.toString());
	}

	private HttpURLConnection openHttpURLConnection(final String method, final String file) throws IOException {

		System.out.println("Host " + this.host + ", port " + this.port);
		final URL url = new URL("http", this.host, this.port, file);
		final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		final String date = this.dateFormat.format(new Date()) + "GMT";

		final String signature = createSignature(method, "", "", date, file);

		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setUseCaches(false);
		conn.setDefaultUseCaches(false);
		conn.setAllowUserInteraction(true);
		conn.setRequestMethod(method);
		conn.setRequestProperty("Date", date);
		conn.setRequestProperty("Content-Length", "100");
		conn.setRequestProperty("Authorization", this.authorizationPrefix + signature);

		return conn;
	}
	
	private void modifyHttpURLConnection(final HttpURLConnection connection, final String method) throws IOException {
		
		
		final String date = this.dateFormat.format(new Date()) + "GMT";
		final String signature = createSignature(method, "", "", date, connection.getURL().getPath());

		connection.setRequestMethod(method);
		connection.setRequestProperty("Date", date);
		connection.setRequestProperty("Authorization", this.authorizationPrefix + signature);
	}
}
