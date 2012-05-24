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

package eu.stratosphere.pact.testing.ioformats;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.ReflectionUtil;

/**
 * Provides convencience methods to deal with I/O operations related to {@link IOReadableWritable}, {@link InputFormat}, and {@link OutputFormat}.
 * 
 * @author Arvid Heise
 */
public class FormatUtil {
	/**
	 * Converts an {@link IOReadableWritable} to a string by writing the result of {@link IOReadableWritable#write(java.io.DataOutput)} into a string. <br>
	 * This method can be used to store smaller {@link IOReadableWritable} data into a {@link Configuration}.
	 *  
	 * @param writable the value to write 
	 * @return the string representing the given value.
	 * @see FormatUtil#fromString(Class, String)
	 */
	public static String toString(IOReadableWritable writable) {
		try {
			ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
			DataOutputStream outStream = new DataOutputStream(byteArray);
			writable.write(outStream);
			outStream.close();
			return byteArray.toString("utf-8");
		} catch (IOException e) {
			throw new RuntimeException("should never happen since we write to an in-memory string", e);
		}
	}

	/**
	 * Converts a string to an {@link IOReadableWritable} by parsing the string with {@link IOReadableWritable#read(java.io.DataInput)}. <br>
	 * This method can be used to restore smaller {@link IOReadableWritable} data from a {@link Configuration}.
	 *  
	 * @param readableClass the class of the value to read 
	 * @param string the string representing the given value.
	 * @param <T> the class of the value to read
	 * @return an instance of T parsed from the string
	 * @see FormatUtil#toString(IOReadableWritable)
	 */
	public static <T extends IOReadableWritable> T fromString(Class<T> readableClass, String string) {
		try {
			ByteArrayInputStream byteArray = new ByteArrayInputStream(Charset.forName("utf-8").encode(string).array());
			DataInputStream inStream = new DataInputStream(byteArray);
			T readableWritable = ReflectionUtil.newInstance(readableClass);
			readableWritable.read(inStream);
			inStream.close();
			return readableWritable;
		} catch (IOException e) {
			throw new RuntimeException("should never happen since we read from an in-memory string", e);
		}
	}

	/**
	 * Creates an {@link InputFormat} from a given class for the specified file. The optional {@link Configuration} initializes the format.  
	 * 
	 * @param <T> the class of the InputFormat
	 * @param inputFormatClass the class of the InputFormat
	 * @param path the path of the file
	 * @param configuration optional configuration of the InputFormat
	 * @return the created {@link InputFormat}
	 * @throws IOException if an I/O error occurred while accessing the file or initializing the InputFormat.
	 */
	public static <T extends FileInputFormat<? extends Key, ? extends Value>> T createInputFormat(
			Class<T> inputFormatClass, String path, Configuration configuration) throws IOException
	{
		configuration = configuration == null ? new Configuration() : configuration;
		
		org.apache.hadoop.fs.Path hadoopPath = normalizePath(new org.apache.hadoop.fs.Path(path));
		final T inputFormat = ReflectionUtil.newInstance(inputFormatClass);
		
		configuration.setString(FileInputFormat.FILE_PARAMETER_KEY, path);
		inputFormat.configure(configuration);
			
		final org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(hadoopPath.toUri(),
			new org.apache.hadoop.conf.Configuration());

		inputFormat.open(new FileInputSplit(0, new Path(path), 0, fs.getFileStatus(hadoopPath).getLen(), new String[] {"localhost"}));
		return inputFormat;
	}

	/**
	 * Creates {@link InputFormat}s from a given class for the specified file(s). The optional {@link Configuration} initializes the formats.  
	 * 
	 * @param <T> the class of the InputFormat
	 * @param inputFormatClass the class of the InputFormat
	 * @param path the path of the file or to the directory containing the splits
	 * @param configuration optional configuration of the InputFormat
	 * @return the created {@link InputFormat}s for each file in the specified path
	 * @throws IOException if an I/O error occurred while accessing the files or initializing the InputFormat.
	 */
	@SuppressWarnings("unchecked")
	public static <T extends FileInputFormat<? extends Key, ? extends Value>> T[] createInputFormats(
			Class<T> inputFormatClass, String path, Configuration configuration) throws IOException {
		Path nephelePath = new Path(path);
		FileSystem fs = nephelePath.getFileSystem();
		FileStatus fileStatus = fs.getFileStatus(nephelePath);
		if (!fileStatus.isDir())
			return (T[]) new FileInputFormat[] { createInputFormat(inputFormatClass, path, configuration) };
		FileStatus[] list = fs.listStatus(nephelePath);
		T[] formats = (T[]) new FileInputFormat[list.length];
		for (int index = 0; index < formats.length; index++)
			formats[index] = createInputFormat(inputFormatClass, list[index].getPath().toString(), configuration);
		return formats;
	}

	/**
	 * Creates an {@link OutputFormat} from a given class for the specified file. The optional {@link Configuration} initializes the format.  
	 * 
	 * @param <T> the class of the OutputFormat
	 * @param outputFormatClass the class of the OutputFormat
	 * @param path the path of the file or to the directory containing the splits
	 * @param configuration optional configuration of the OutputFormat
	 * @return the created {@link OutputFormat}
	 * @throws IOException if an I/O error occurred while accessing the file or initializing the OutputFormat.
	 */
	public static <T extends FileOutputFormat<? extends Key, ? extends Value>> T createOutputFormat(
			Class<T> outputFormatClass, String path, Configuration configuration) throws IOException
	{
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
	private static org.apache.hadoop.fs.Path normalizePath(org.apache.hadoop.fs.Path path) {
		URI uri = path.toUri();
		if (uri.getScheme() == null) {
			try {
				uri = new URI("file", uri.getHost(), uri.getPath(), uri.getFragment());
				path = new org.apache.hadoop.fs.Path(uri.toString());
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("path is invalid", e);
			}
		}
		return path;
	}
}
