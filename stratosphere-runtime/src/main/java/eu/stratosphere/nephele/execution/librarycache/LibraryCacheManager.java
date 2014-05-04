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

package eu.stratosphere.nephele.execution.librarycache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FSDataOutputStream;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.util.StringUtils;

/**
 * For each job graph that is submitted to the system the library cache manager maintains
 * a set of libraries (typically JAR files) which the job requires to run. The library cache manager
 * caches library files in order to avoid unnecessary retransmission of data. It is based on a singleton
 * programming pattern, so there exists at most on library manager at a time.
 * <p>
 * This class is thread-safe.
 * 
 */
public final class LibraryCacheManager {

	/**
	 * The instance of the library cache manager accessible through a singleton pattern.
	 */
	private static LibraryCacheManager libraryManager = null;

	/**
	 * Map to translate client paths of libraries to the file name used by the cache manager.
	 */
	private final ConcurrentMap<LibraryTranslationKey, String> clientPathToCacheName = new ConcurrentHashMap<LibraryTranslationKey, String>();

	/**
	 * Name of the directory to put cached libraries in.
	 */
	private static final String LIBRARYCACHENAME = "libraryCache";

	/**
	 * Algorithm to be used for calculating the checksum of the libraries.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	/**
	 * Dummy object used in the lock map.
	 */
	private static final Object LOCK_OBJECT = new Object();

	/**
	 * File system object used to access the local file system.
	 */
	private final FileSystem fs;

	/**
	 * The message digest object used to calculate the checksums of the libraries.
	 */
	private final MessageDigest md;

	/**
	 * Path pointing to the library cache directory.
	 */
	private final Path libraryCachePath;

	/**
	 * Map to translate a job ID to the responsible library cache manager entry.
	 */
	private final ConcurrentMap<JobID, LibraryManagerEntry> libraryManagerEntries = new ConcurrentHashMap<JobID, LibraryManagerEntry>();

	/**
	 * Map to store the number of references to a specific library manager entry.
	 */
	private final ConcurrentMap<JobID, AtomicInteger> libraryReferenceCounter = new ConcurrentHashMap<JobID, AtomicInteger>();

	/**
	 * Map to guarantee atomicity of of register/unregister operations.
	 */
	private final ConcurrentMap<JobID, Object> lockMap = new ConcurrentHashMap<JobID, Object>();

	/**
	 * Returns the singleton instance of the library cache manager.
	 * 
	 * @return the singleton instance of the library cache manager.
	 * @throws IOException
	 *         thrown if access to the file system can not be obtained or the requested hashing algorithm does not exist
	 */
	private static synchronized LibraryCacheManager get() throws IOException {

		// Lazy initialization
		if (libraryManager == null) {
			libraryManager = new LibraryCacheManager();
		}

		return libraryManager;
	}

	/**
	 * Constructs a new instance of the library cache manager.
	 * 
	 * @throws IOException
	 *         thrown if access to the file system can not be obtained or the requested hashing algorithm does not exist
	 */
	private LibraryCacheManager() throws IOException {

		// Check if the library cache directory exists, otherwise create it
		final String tmp = System.getProperty("java.io.tmpdir");
		if (tmp == null) {
			throw new IOException("Cannot find directory for temporary files");
		}

		this.fs = FileSystem.getLocalFileSystem();

		// Add user name of library cache path to avoid conflicts with regard to file permissions
		String userName = System.getProperty("user.name");
		if (userName == null) {
			userName = "default";
		}

		// Use the File object to the convert the path to a proper URI
		final File path = new File(tmp + File.separator + LIBRARYCACHENAME + "-" + userName);
		final URI uri = path.toURI();

		this.libraryCachePath = new Path(uri);

		this.fs.mkdirs(this.libraryCachePath);

		// Create an MD5 message digest object we can use
		try {
			this.md = MessageDigest.getInstance(HASHING_ALGORITHM);
		} catch (NoSuchAlgorithmException e) {
			throw new IOException("Cannot find algorithm " + HASHING_ALGORITHM + ": "
				+ StringUtils.stringifyException(e));
		}
	}

	/**
	 * Increments the reference counter for the library manager entry with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID identifying the library manager entry
	 * @return the increased reference counter
	 */
	private int incrementReferenceCounter(final JobID jobID) {

		while (true) {

			AtomicInteger ai = this.libraryReferenceCounter.get(jobID);
			if (ai == null) {

				ai = new AtomicInteger(1);
				if (this.libraryReferenceCounter.putIfAbsent(jobID, ai) == null) {
					return 1;
				}

				// We had a race, try again
			} else {
				return ai.incrementAndGet();
			}
		}
	}

	/**
	 * Decrements the reference counter for the library manager entry with the given job ID.
	 * 
	 * @param jobID
	 *        the job ID identifying the library manager entry
	 * @return the decremented reference counter
	 */
	private int decrementReferenceCounter(final JobID jobID) {

		final AtomicInteger ai = this.libraryReferenceCounter.get(jobID);

		if (ai == null) {
			throw new IllegalStateException("Cannot find reference counter entry for job " + jobID);
		}

		int retVal = ai.decrementAndGet();

		if (retVal == 0) {
			this.libraryReferenceCounter.remove(jobID);
		}

		return retVal;
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. The library paths are given in
	 * terms
	 * of client paths, so the method first translates the client paths into the corresponding internal cache names. For
	 * every registered
	 * job the library cache manager creates a class loader that is used to instantiate the job's environment later on.
	 * 
	 * @param id
	 *        the ID of the job to be registered
	 * @param clientPaths
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated, no mapping between the job ID and a job ID
	 *         exists or the requested library is not in the cache.
	 */
	public static void register(final JobID id, final Path[] clientPaths) throws IOException {

		final LibraryCacheManager lib = get();
		lib.registerInternal(id, clientPaths);
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. The library paths are given in
	 * terms
	 * of client paths, so the method first translates the client paths into the corresponding internal cache names. For
	 * every registered
	 * job the library cache manager creates a class loader that is used to instantiate the job's environment later on.
	 * 
	 * @param id
	 *        the ID of the job to be registered.
	 * @param clientPaths
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if no mapping between the job ID and a job ID exists or the requested library is not in the cache.
	 */
	private void registerInternal(final JobID id, final Path[] clientPaths) throws IOException {

		final String[] cacheNames = new String[clientPaths.length];

		for (int i = 0; i < clientPaths.length; ++i) {
			final LibraryTranslationKey key = new LibraryTranslationKey(id, clientPaths[i]);
			cacheNames[i] = this.clientPathToCacheName.get(key);
			if (cacheNames[i] == null) {
				throw new IOException("Cannot map" + clientPaths[i].toString() + " to cache name");
			}
		}

		// Register as regular
		registerInternal(id, cacheNames);
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. For every registered
	 * job the library cache manager creates a class loader that is used to instantiate the job's environment later on.
	 * 
	 * @param id
	 *        the ID of the job to be registered.
	 * @param clientPaths
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated or one of the requested libraries is not in
	 *         the cache
	 */
	public static void register(final JobID id, final String[] requiredJarFiles) throws IOException {

		final LibraryCacheManager lib = get();
		lib.registerInternal(id, requiredJarFiles);
	}

	/**
	 * Registers a job ID with a set of library paths that are required to run the job. For every registered
	 * job the library cache manager creates a class loader that is used to instantiate the vertex's environment later
	 * on.
	 * 
	 * @param id
	 *        the ID of the job to be registered.
	 * @param clientPaths
	 *        the client path's of the required libraries
	 * @throws IOException
	 *         thrown if one of the requested libraries is not in the cache
	 */
	private void registerInternal(final JobID id, final String[] requiredJarFiles) throws IOException {

		// Use spin lock here
		while (this.lockMap.putIfAbsent(id, LOCK_OBJECT) != null);

		try {
			if (incrementReferenceCounter(id) > 1) {
				return;
			}

			// Check if library manager entry for this id already exists
			if (this.libraryManagerEntries.containsKey(id)) {
				throw new IllegalStateException("Library cache manager already contains entry for job ID " + id);
			}

			// Check if all the required jar files exist in the cache
			URL[] urls = null;
			if (requiredJarFiles != null) {

				urls = new URL[requiredJarFiles.length];

				for (int i = 0; i < requiredJarFiles.length; i++) {
					final Path p = contains(requiredJarFiles[i]);
					if (p == null) {
						throw new IOException(requiredJarFiles[i] + " does not exist in the library cache");
					}

					// Add file to the URL array
					try {
						urls[i] = p.toUri().toURL();
					} catch (MalformedURLException e) {
						throw new IOException(StringUtils.stringifyException(e));
					}
				}
			}

			final LibraryManagerEntry entry = new LibraryManagerEntry(id, requiredJarFiles, urls);

			this.libraryManagerEntries.put(id, entry);

		} finally {
			this.lockMap.remove(id);
		}
	}

	/**
	 * Unregisters a job ID and releases the resources associated with it.
	 * 
	 * @param id
	 *        the job ID to unregister
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated
	 */
	public static void unregister(final JobID id) throws IOException {

		final LibraryCacheManager lib = get();
		lib.unregisterInternal(id);
	}

	/**
	 * Unregisters a job ID and releases the resources associated with it.
	 * 
	 * @param id
	 *        the job ID to unregister
	 */
	private void unregisterInternal(final JobID id) {

		// Use spin lock here
		while (this.lockMap.putIfAbsent(id, LOCK_OBJECT) != null);

		if (decrementReferenceCounter(id) == 0) {
			this.libraryManagerEntries.remove(id);
		}

		this.lockMap.remove(id);
	}

	/**
	 * Checks if the given library is in the local cache.
	 * 
	 * @param cacheName
	 *        The name of the library to be checked for.
	 * @return the path object of the library if it is cached, <code>null</code> otherwise
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated or no access to the file system could be
	 *         obtained
	 */
	public static Path contains(final String cacheName) throws IOException {

		final LibraryCacheManager lib = get();
		return lib.containsInternal(cacheName);
	}

	/**
	 * Checks if the given library is in the local cache.
	 * 
	 * @param cacheName
	 *        The name of the library to be checked for.
	 * @return the path object of the library if it is cached, <code>null</code> otherwise
	 * @throws IOException
	 *         thrown if no access to the file system could be obtained
	 */
	private Path containsInternal(final String cacheName) throws IOException {

		// Create a path object from the external name string
		final Path p = new Path(this.libraryCachePath + File.separator + cacheName);

		synchronized (this.fs) {
			if (fs.exists(p)) {
				return p;
			}
		}

		return null;
	}

	/**
	 * Returns the class loader to the specified vertex.
	 * 
	 * @param id
	 *        the ID of the job to return the class loader for
	 * @return the class loader of requested vertex or <code>null</code> if no class loader has been registered with the
	 *         given ID.
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated
	 */
	public static ClassLoader getClassLoader(final JobID id) throws IOException {

		if (id == null) {
			return null;
		}

		final LibraryCacheManager lib = get();
		return lib.getClassLoaderInternal(id);
	}

	/**
	 * Returns the class loader to the specified vertex.
	 * 
	 * @param id
	 *        the ID of the job to return the class loader for
	 * @return the class loader of requested vertex or <code>null</code> if no class loader has been registered with the
	 *         given ID.
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated
	 */
	private ClassLoader getClassLoaderInternal(final JobID id) {

		final LibraryManagerEntry entry = this.libraryManagerEntries.get(id);

		if (entry == null) {
			return null;
		}

		return entry.getClassLoader();
	}

	/**
	 * Returns the names of the required libraries of the specified job.
	 * 
	 * @param id
	 *        the ID of the job to return the names of required libraries for.
	 * @return the names of the required libraries or <code>null</code> if the specified job ID is unknown
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated
	 */
	public static String[] getRequiredJarFiles(final JobID id) throws IOException {

		if (id == null) {
			return new String[0];
		}

		final LibraryCacheManager lib = get();

		return lib.getRequiredJarFilesInternal(id);
	}

	/**
	 * Returns the names of the required libraries of the specified job.
	 * 
	 * @param id
	 *        the ID of the job to return the names of required libraries for.
	 * @return the names of the required libraries or <code>null</code> if the specified job ID is unknown
	 */
	private String[] getRequiredJarFilesInternal(final JobID id) {

		final LibraryManagerEntry entry = this.libraryManagerEntries.get(id);

		if (entry == null) {
			return null;
		}

		return entry.getRequiredJarFiles();
	}

	/**
	 * Writes data from the library with the given file name to the specified stream.
	 * 
	 * @param libraryFileName
	 *        the name of the library
	 * @param out
	 *        the stream to write the data to
	 * @throws IOException
	 *         thrown if an error occurs while writing the data
	 */
	public static void writeLibraryToStream(final String libraryFileName, final DataOutput out) throws IOException {

		final LibraryCacheManager lib = get();
		lib.writeLibraryToStreamInternal(libraryFileName, out);

	}

	/**
	 * Writes data from the library with the given file name to the specified stream.
	 * 
	 * @param libraryFileName
	 *        the name of the library
	 * @param out
	 *        the stream to write the data to
	 * @throws IOException
	 *         thrown if an error occurs while writing the data
	 */
	private void writeLibraryToStreamInternal(final String libraryFileName, final DataOutput out) throws IOException {

		if (libraryFileName == null) {
			throw new IOException("libraryName is null!");
		}

		final Path storePath = new Path(this.libraryCachePath + "/" + libraryFileName);

		synchronized (this.fs) {

			if (!fs.exists(storePath)) {
				throw new IOException(storePath + " does not exist!");
			}

			final FileStatus status = fs.getFileStatus(storePath);

			StringRecord.writeString(out, libraryFileName);
			out.writeLong(status.getLen());

			final FSDataInputStream inStream = fs.open(storePath);
			final byte[] buf = new byte[8192]; // 8K Buffer*/
			int read = inStream.read(buf, 0, buf.length);
			while (read > 0) {
				out.write(buf, 0, read);
				read = inStream.read(buf, 0, buf.length);
			}

			inStream.close();
		}
	}

	/**
	 * Reads library data from the given stream.
	 * 
	 * @param in
	 *        the stream to read the library data from
	 * @throws IOException
	 *         throws if an error occurs while reading from the stream
	 */
	public static void readLibraryFromStream(final DataInput in) throws IOException {

		final LibraryCacheManager lib = get();
		lib.readLibraryFromStreamInternal(in);

	}

	/**
	 * Reads library data from the given stream.
	 * 
	 * @param in
	 *        the stream to read the library data from
	 * @throws IOException
	 *         throws if an error occurs while reading from the stream
	 */
	private void readLibraryFromStreamInternal(final DataInput in) throws IOException {

		final String libraryFileName = StringRecord.readString(in);

		if (libraryFileName == null) {
			throw new IOException("libraryFileName is null!");
		}

		final long length = in.readLong();

		if (length > (long) Integer.MAX_VALUE) {
			throw new IOException("Submitted jar file " + libraryFileName + " is too large");
		}

		final byte[] buf = new byte[(int) length];
		in.readFully(buf);

		final Path storePath = new Path(this.libraryCachePath + "/" + libraryFileName);

		synchronized (this.fs) {

			// Check if file already exists in our library cache, if not write it to the cache directory
			if (!fs.exists(storePath)) {
				final FSDataOutputStream fos = fs.create(storePath, false);
				fos.write(buf, 0, buf.length);
				fos.close();
			}
		}
	}

	/**
	 * Reads a library from the given input stream and adds it to the local library cache. The cache name of
	 * the library is determined by the checksum of the received data and cannot be specified manually.
	 * 
	 * @param jobID
	 *        the ID of the job the library data belongs to
	 * @param name
	 *        the name of the library at the clients host
	 * @param size
	 *        the size of the library to be read from the input stream
	 * @param in
	 *        the data input stream
	 * @throws IOException
	 *         thrown if the library cache manager could not be instantiated or an error occurred while reading the
	 *         library data from the input stream
	 */
	public static void addLibrary(final JobID jobID, final Path name, final long size, final DataInput in)
			throws IOException {

		final LibraryCacheManager lib = get();
		lib.addLibraryInternal(jobID, name, size, in);
	}

	/**
	 * Reads a library from the given input stream and adds it to the local library cache. The cache name of
	 * the library is determined by the checksum of the received data and cannot be specified manually.
	 * 
	 * @param jobID
	 *        the ID of the job the library data belongs to
	 * @param name
	 *        the name of the library at the clients host
	 * @param size
	 *        the size of the library to be read from the input stream
	 * @param in
	 *        the data input stream
	 * @throws IOException
	 *         thrown if an error occurred while reading the library data from the input stream
	 */
	private void addLibraryInternal(final JobID jobID, final Path name, final long size, final DataInput in)
			throws IOException {

		if (size > (long) Integer.MAX_VALUE) {
			throw new IOException("Submitted jar file " + name + " is too large");
		}

		// Map the entire jar file to memory
		final byte[] buf = new byte[(int) size];
		in.readFully(buf);

		// Reset and calculate message digest from jar file
		this.md.reset();
		this.md.update(buf);

		// Construct internal jar name from digest
		final String cacheName = StringUtils.byteToHexString(md.digest()) + ".jar";
		final Path storePath = new Path(this.libraryCachePath + "/" + cacheName);

		synchronized (this.fs) {

			// Check if file already exists in our library cache, if not write it to the cache directory
			if (!this.fs.exists(storePath)) {
				final FSDataOutputStream fos = this.fs.create(storePath, false);
				fos.write(buf, 0, buf.length);
				fos.close();
			}
		}

		// Create mapping for client path and cache name
		final LibraryTranslationKey key = new LibraryTranslationKey(jobID, name);
		this.clientPathToCacheName.putIfAbsent(key, cacheName);
	}

	/**
	 * Auxiliary class that stores the class loader object as well as the names of the required
	 * libraries for a job vertex.
	 * 
	 */
	private static class LibraryManagerEntry {

		/**
		 * The class loader object for the Nephele job this object belongs to.
		 */
		private final ClassLoader classLoader;

		/**
		 * A list containing the names of the JAR files required by the Nephele job this object belongs to.
		 */
		private final String[] requiredJarFiles;

		/**
		 * Constructs a <code>LibraryManagerEntry</code> object from the given job ID and array of required library
		 * files.
		 * 
		 * @param id
		 *        the ID of the job to create a <code>LibraryManagerEntry</code> for.
		 * @param requiredJarFiles
		 *        an array with the names of required libraries by the corresponding job (plain names)
		 * @param urls
		 *        an array with the names of required libraries by the corresponding job (URL objects required by the
		 *        class loader)
		 */
		public LibraryManagerEntry(final JobID id, final String[] requiredJarFiles, URL[] urls) {

			String[] temp = requiredJarFiles;
			if (temp == null) {
				temp = new String[0];
			}

			this.requiredJarFiles = temp;

			if (urls == null) {
				urls = new URL[0];
			}

			this.classLoader = new URLClassLoader(urls, ClassLoader.getSystemClassLoader());
		}

		/**
		 * Returns the class loader associated with this library manager entry.
		 * 
		 * @return the class loader associated with this library manager entry
		 */
		public ClassLoader getClassLoader() {
			return this.classLoader;
		}

		/**
		 * Returns a (possibly empty) array of library names required by the associated job vertex to run.
		 * 
		 * @return a (possibly empty) array of library names required by the associated job vertex to run
		 */
		public String[] getRequiredJarFiles() {
			return this.requiredJarFiles;
		}
	}

	/**
	 * Auxiliary class that acts as a key for the translation of the names a client uses to refer to required libraries
	 * for a vertex
	 * and the internal names used by the library cache manager.
	 * 
	 */
	private static class LibraryTranslationKey {

		/**
		 * The ID of the job this object belongs to.
		 */
		private final JobID jobID;

		/**
		 * The path at which the library has been stored at the client.
		 */
		private final Path clientPath;

		/**
		 * Construct a <code>LibraryTranslationKey</code> object from a fiven job ID and a client path that specifies
		 * the name of
		 * required library at the job client.
		 * 
		 * @param jobID
		 *        the job ID
		 * @param clientPath
		 *        the client path
		 */
		public LibraryTranslationKey(final JobID jobID, final Path clientPath) {

			this.jobID = jobID;
			this.clientPath = clientPath;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {

			final long temp = (this.jobID.hashCode() + this.clientPath.hashCode()) % Integer.MAX_VALUE;

			return (int) temp;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(final Object obj) {

			if (obj == null) {
				return false;
			}

			if (this.jobID == null) {
				return false;
			}

			if (this.clientPath == null) {
				return false;
			}

			if (obj instanceof LibraryTranslationKey) {

				final LibraryTranslationKey key = (LibraryTranslationKey) obj;
				if (this.jobID.equals(key.getJobID()) && this.clientPath.equals(key.getClientPath())) {
					return true;
				}
			}

			return false;
		}

		/**
		 * Returns the client path associated with this object.
		 * 
		 * @return the client path associated with this object
		 */
		public Path getClientPath() {
			return this.clientPath;
		}

		/**
		 * Returns the job ID associated with this object.
		 * 
		 * @return the job ID associated with this object
		 */
		public JobID getJobID() {
			return this.jobID;
		}
	}
}
