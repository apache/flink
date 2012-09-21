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

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Names a file or directory in a {@link FileSystem}. Path strings use slash as
 * the directory separator. A path string is absolute if it begins with a slash.
 */
public final class Path implements KryoSerializable {

	/**
	 * The directory separator, a slash.
	 */
	public static final String SEPARATOR = "/";

	/**
	 * The directory separator, a slash (character).
	 */
	public static final char SEPARATOR_CHAR = '/';

	/**
	 * Character denoting the current directory.
	 */
	public static final String CUR_DIR = ".";

	/**
	 * Stores <code>true</code> if the host operation system is Windows, <code>false</code> otherwise.
	 */
	static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

	/**
	 * The internal representation of the path, a hierarchical URI.
	 */
	private URI uri = null;

	/**
	 * Constructs a new (empty) path object (used to reconstruct path object after RPC call).
	 */
	public Path() {
	}

	/**
	 * Constructs a path object from a given URI.
	 * 
	 * @param uri
	 *        the URI to contruct the path object from
	 */
	public Path(final URI uri) {
		this.uri = uri;
	}

	/**
	 * Resolve a child path against a parent path.
	 * 
	 * @param parent
	 *        the parent path
	 * @param child
	 *        the child path
	 */
	public Path(final String parent, final String child) {
		this(new Path(parent), new Path(child));
	}

	/**
	 * Resolve a child path against a parent path.
	 * 
	 * @param parent
	 *        the parent path
	 * @param child
	 *        the child path
	 */
	public Path(final Path parent, final String child) {
		this(parent, new Path(child));
	}

	/**
	 * Resolve a child path against a parent path.
	 * 
	 * @param parent
	 *        the parent path
	 * @param child
	 *        the child path
	 */
	public Path(final String parent, final Path child) {
		this(new Path(parent), child);
	}

	/**
	 * Resolve a child path against a parent path.
	 * 
	 * @param parent
	 *        the parent path
	 * @param child
	 *        the child path
	 */
	public Path(final Path parent, Path child) {
		// Add a slash to parent's path so resolution is compatible with URI's
		URI parentUri = parent.uri;
		final String parentPath = parentUri.getPath();
		if (!(parentPath.equals("/") || parentPath.equals(""))) {
			try {
				parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri.getPath() + "/", null,
					null);
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException(e);
			}
		}

		if (child.uri.getPath().startsWith(Path.SEPARATOR)) {
			child = new Path(child.uri.getScheme(), child.uri.getAuthority(), child.uri.getPath().substring(1));
		}

		final URI resolved = parentUri.resolve(child.uri);
		initialize(resolved.getScheme(), resolved.getAuthority(), normalizePath(resolved.getPath()));
	}

	/**
	 * Checks if the provided path string is either null or has zero length and throws
	 * a {@link IllegalArgumentException} if any of the two conditions apply.
	 * 
	 * @param path
	 *        the path string to be checked
	 */
	private void checkPathArg(final String path) {
		// disallow construction of a Path from an empty string
		if (path == null) {
			throw new IllegalArgumentException("Can not create a Path from a null string");
		}
		if (path.length() == 0) {
			throw new IllegalArgumentException("Can not create a Path from an empty string");
		}
	}

	/**
	 * Construct a path from a String. Path strings are URIs, but with unescaped
	 * elements and some additional normalization.
	 * 
	 * @param pathString
	 *        the string to construct a path from
	 */
	public Path(String pathString) {
		checkPathArg(pathString);

		// We can't use 'new URI(String)' directly, since it assumes things are
		// escaped, which we don't require of Paths.

		// add a slash in front of paths with Windows drive letters
		if (hasWindowsDrive(pathString, false)) {
			pathString = "/" + pathString;
		}

		// parse uri components
		String scheme = null;
		String authority = null;

		int start = 0;

		// parse uri scheme, if any
		final int colon = pathString.indexOf(':');
		final int slash = pathString.indexOf('/');
		if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
			// scheme
			scheme = pathString.substring(0, colon);
			start = colon + 1;
		}

		// parse uri authority, if any
		if (pathString.startsWith("//", start) && (pathString.length() - start > 2)) { // has authority
			final int nextSlash = pathString.indexOf('/', start + 2);
			final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
			authority = pathString.substring(start + 2, authEnd);
			start = authEnd;
		}

		// uri path is the rest of the string -- query & fragment not supported
		final String path = pathString.substring(start, pathString.length());

		initialize(scheme, authority, path);
	}

	/**
	 * Construct a Path from a scheme, an authority and a path string.
	 * 
	 * @param scheme
	 *        the scheme string
	 * @param authority
	 *        the authority string
	 * @param path
	 *        the path string
	 */
	public Path(final String scheme, final String authority, final String path) {
		checkPathArg(path);
		initialize(scheme, authority, path);
	}

	/**
	 * Initializes a path object given the scheme, authority and path string.
	 * 
	 * @param scheme
	 *        the scheme string.
	 * @param authority
	 *        the authority string.
	 * @param path
	 *        the path string.
	 */
	private void initialize(final String scheme, final String authority, final String path) {
		try {
			this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * Normalizes a path string.
	 * 
	 * @param path
	 *        the path string to normalize
	 * @return the normalized path string
	 */
	private String normalizePath(String path) {
		// remove double slashes & backslashes
		path = path.replace("//", "/");
		path = path.replace("\\", "/");

		return path;
	}

	/**
	 * Checks if the provided path string contains a windows drive letter.
	 * 
	 * @param path
	 *        the path to check
	 * @param slashed
	 *        <code>true</code> to indicate the first character of the string is a slash, <code>false</code> otherwise
	 * @return <code>true</code> if the path string contains a windows drive letter, <code>false</code> otherwise
	 */
	private boolean hasWindowsDrive(final String path, final boolean slashed) {
		if (!WINDOWS) {
			return false;
		}
		final int start = slashed ? 1 : 0;
		return path.length() >= start + 2
			&& (slashed ? path.charAt(0) == '/' : true)
			&& path.charAt(start + 1) == ':'
			&& ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') || (path.charAt(start) >= 'a' && path
				.charAt(start) <= 'z'));
	}

	/**
	 * Converts the path object to a {@link URI}.
	 * 
	 * @return the {@link URI} object converted from the path object
	 */
	public URI toUri() {
		return uri;
	}

	/**
	 * Returns the FileSystem that owns this Path.
	 * 
	 * @return the FileSystem that owns this Path
	 * @throws IOException
	 *         thrown if the file system could not be retrieved
	 */
	public FileSystem getFileSystem() throws IOException {
		return FileSystem.get(this.toUri());
	}

	/**
	 * Checks if the directory of this path is absolute.
	 * 
	 * @return <code>true</code> if the directory of this path is absolute, <code>false</code> otherwise
	 */
	public boolean isAbsolute() {
		final int start = hasWindowsDrive(uri.getPath(), true) ? 3 : 0;
		return uri.getPath().startsWith(SEPARATOR, start);
	}

	/**
	 * Returns the final component of this path.
	 * 
	 * @return the final component of the path
	 */
	public String getName() {
		final String path = uri.getPath();
		final int slash = path.lastIndexOf(SEPARATOR);
		return path.substring(slash + 1);
	}

	/**
	 * Returns the parent of a path or <code>null</code> if at root.
	 * 
	 * @return the parent of a path or <code>null</code> if at root.
	 */
	public Path getParent() {
		final String path = uri.getPath();
		final int lastSlash = path.lastIndexOf('/');
		final int start = hasWindowsDrive(path, true) ? 3 : 0;
		if ((path.length() == start) || // empty path
			(lastSlash == start && path.length() == start + 1)) { // at root
			return null;
		}
		String parent;
		if (lastSlash == -1) {
			parent = CUR_DIR;
		} else {
			final int end = hasWindowsDrive(path, true) ? 3 : 0;
			parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
		}
		return new Path(uri.getScheme(), uri.getAuthority(), parent);
	}

	/**
	 * Adds a suffix to the final name in the path.
	 * 
	 * @param the
	 *        suffix to be added
	 * @return the new path including the suffix
	 */
	public Path suffix(final String suffix) {
		return new Path(getParent(), getName() + suffix);
	}

	@Override
	public String toString() {
		// we can't use uri.toString(), which escapes everything, because we
		// want
		// illegal characters unescaped in the string, for glob processing, etc.
		final StringBuffer buffer = new StringBuffer();
		if (uri.getScheme() != null) {
			buffer.append(uri.getScheme());
			buffer.append(":");
		}
		if (uri.getAuthority() != null) {
			buffer.append("//");
			buffer.append(uri.getAuthority());
		}
		if (uri.getPath() != null) {
			String path = uri.getPath();
			if (path.indexOf('/') == 0 && hasWindowsDrive(path, true) && // has
				// windows
				// drive
				uri.getScheme() == null && // but no scheme
				uri.getAuthority() == null) { // or authority
				path = path.substring(1); // remove slash before drive
			}
			buffer.append(path);
		}
		return buffer.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof Path)) {
			return false;
		}
		final Path that = (Path) o;
		return this.uri.equals(that.uri);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return uri.hashCode();
	}

	public int compareTo(final Object o) {
		final Path that = (Path) o;
		return this.uri.compareTo(that.uri);
	}

	/**
	 * Returns the number of elements in this path.
	 * 
	 * @return the number of elements in this path
	 */
	public int depth() {
		String path = uri.getPath();
		int depth = 0;
		int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
		while (slash != -1) {
			depth++;
			slash = path.indexOf(SEPARATOR, slash + 1);
		}
		return depth;
	}

	/**
	 * Returns a qualified path object.
	 * 
	 * @param fs
	 *        the FileSystem that should be used to obtain the current working directory
	 * @return the qualified path object
	 */
	public Path makeQualified(final FileSystem fs) {
		Path path = this;
		if (!isAbsolute()) {
			path = new Path(fs.getWorkingDirectory(), this);
		}

		final URI pathUri = path.toUri();
		final URI fsUri = fs.getUri();

		String scheme = pathUri.getScheme();
		String authority = pathUri.getAuthority();

		if (scheme != null && (authority != null || fsUri.getAuthority() == null))
			return path;

		if (scheme == null) {
			scheme = fsUri.getScheme();
		}

		if (authority == null) {
			authority = fsUri.getAuthority();
			if (authority == null) {
				authority = "";
			}
		}

		return new Path(scheme + ":" + "//" + authority + pathUri.getPath());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		if (this.uri == null) {
			output.writeBoolean(false);
		} else {
			output.writeBoolean(true);
			output.writeString(this.uri.getScheme());
			output.writeString(this.uri.getUserInfo());
			output.writeString(this.uri.getHost());
			output.writeInt(this.uri.getPort());
			output.writeString(this.uri.getPath());
			output.writeString(this.uri.getQuery());
			output.writeString(this.uri.getFragment());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		final boolean isNotNull = input.readBoolean();
		if (isNotNull) {
			final String scheme = input.readString();
			final String userInfo = input.readString();
			final String host = input.readString();
			final int port = input.readInt();
			final String path = input.readString();
			final String query = input.readString();
			final String fragment = input.readString();

			try {
				this.uri = new URI(scheme, userInfo, host, port, path, query, fragment);
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
