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

package eu.stratosphere.pact.common.io;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * Provides convenience methods to deal with I/O operations related to {@link InputFormat} and {@link OutputFormat}.
 * 
 * @author Arvid Heise
 */
public class FormatUtil {

	/**
	 * Creates an {@link InputFormat} from a given class for the specified file. The optional {@link Configuration}
	 * initializes the format.
	 * 
	 * @param <T>
	 *        the class of the InputFormat
	 * @param inputFormatClass
	 *        the class of the InputFormat
	 * @param path
	 *        the path of the file
	 * @param configuration
	 *        optional configuration of the InputFormat
	 * @return the created {@link InputFormat}
	 * @throws IOException
	 *         if an I/O error occurred while accessing the file or initializing the InputFormat.
	 */
	public static <T extends FileInputFormat> T openInput(
			Class<T> inputFormatClass, String path, Configuration configuration) throws IOException {
		configuration = configuration == null ? new Configuration() : configuration;

		Path normalizedPath = normalizePath(new Path(path));
		final T inputFormat = ReflectionUtil.newInstance(inputFormatClass);

		configuration.setString(FileInputFormat.FILE_PARAMETER_KEY, path);
		inputFormat.configure(configuration);

		final FileSystem fs = FileSystem.get(normalizedPath.toUri());
		FileStatus fileStatus = fs.getFileStatus(normalizedPath);

		BlockLocation[] blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		inputFormat.open(new FileInputSplit(0, new Path(path), 0, fileStatus.getLen(), blocks[0].getHosts()));
		return inputFormat;
	}

	/**
	 * Creates {@link InputFormat}s from a given class for the specified file(s). The optional {@link Configuration}
	 * initializes the formats.
	 * 
	 * @param <T>
	 *        the class of the InputFormat
	 * @param inputFormatClass
	 *        the class of the InputFormat
	 * @param path
	 *        the path of the file or to the directory containing the splits
	 * @param configuration
	 *        optional configuration of the InputFormat
	 * @return the created {@link InputFormat}s for each file in the specified path
	 * @throws IOException
	 *         if an I/O error occurred while accessing the files or initializing the InputFormat.
	 */
	@SuppressWarnings("unchecked")
	public static <T extends FileInputFormat> T[] openAllInputs(
			Class<T> inputFormatClass, String path, Configuration configuration) throws IOException {
		Path nephelePath = new Path(path);
		FileSystem fs = nephelePath.getFileSystem();
		FileStatus fileStatus = fs.getFileStatus(nephelePath);
		if (!fileStatus.isDir())
			return (T[]) new FileInputFormat[] { openInput(inputFormatClass, path, configuration) };
		FileStatus[] list = fs.listStatus(nephelePath);
		T[] formats = (T[]) new FileInputFormat[list.length];
		for (int index = 0; index < formats.length; index++)
			formats[index] = openInput(inputFormatClass, list[index].getPath().toString(), configuration);
		return formats;
	}

	/**
	 * Creates an {@link OutputFormat} from a given class for the specified file. The optional {@link Configuration}
	 * initializes the format.
	 * 
	 * @param <T>
	 *        the class of the OutputFormat
	 * @param outputFormatClass
	 *        the class of the OutputFormat
	 * @param path
	 *        the path of the file or to the directory containing the splits
	 * @param configuration
	 *        optional configuration of the OutputFormat
	 * @return the created {@link OutputFormat}
	 * @throws IOException
	 *         if an I/O error occurred while accessing the file or initializing the OutputFormat.
	 */
	public static <T extends FileOutputFormat> T openOutput(
			Class<T> outputFormatClass, String path, Configuration configuration) throws IOException {
		final T outputFormat = ReflectionUtil.newInstance(outputFormatClass);

		configuration = configuration == null ? new Configuration() : configuration;

		configuration.setString(FileOutputFormat.FILE_PARAMETER_KEY, path);
		outputFormat.configure(configuration);
		outputFormat.open(1);
		return outputFormat;
	}

	/**
	 * Fixes the path if it denotes a local (relative) file without the proper protocol prefix.
	 */
	private static Path normalizePath(Path path) {
		URI uri = path.toUri();
		if (uri.getScheme() == null) {
			try {
				uri = new URI("file", uri.getHost(), uri.getPath(), uri.getFragment());
				path = new Path(uri.toString());
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("path is invalid", e);
			}
		}
		return path;
	}
}
