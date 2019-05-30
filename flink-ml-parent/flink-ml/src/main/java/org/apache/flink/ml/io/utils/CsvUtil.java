/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.io.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.MLSession;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import org.apache.commons.io.FileUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * Csv Util.
 */
public class CsvUtil {

	public static String getFileProtocol(String filePath) {
		String protocol = null;
		URL url = null;

		try {
			url = new URL(filePath);
			protocol = url.getProtocol();
		} catch (Exception e) {
			protocol = "";
		}

		return protocol;
	}

	public static byte[] readHttpFile(String filePath) {
		try {
			HttpURLConnection connection;
			URL url = new URL(filePath);
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoInput(true);
			connection.setConnectTimeout(5000);
			connection.setReadTimeout(60000);
			connection.setRequestMethod("GET");
			connection.connect();
			int contentLength = connection.getContentLength();
			InputStream in = connection.getInputStream();
			byte[] buffer = new byte[contentLength];

			int read;
			int totRead = 0;
			while ((read = in.read(buffer, totRead, contentLength - totRead)) != -1) {
				totRead += read;
			}
			connection.disconnect();

			return buffer;
		} catch (Exception e) {
			throw new RuntimeException("Fail to read from: " + filePath + ", mssg: " +
				e.getMessage());
		}
	}

	public static String[] getColNames(String schemaStr) {
		String[] fields = schemaStr.split(",");
		String[] colNames = new String[fields.length];
		for (int i = 0; i < colNames.length; i++) {
			String[] kv = fields[i].trim().split("\\s+");
			colNames[i] = kv[0];
		}
		return colNames;
	}

	public static TypeInformation[] getColTypes(String schemaStr) {
		String[] fields = schemaStr.split(",");
		TypeInformation[] colTypes = new TypeInformation[fields.length];
		for (int i = 0; i < colTypes.length; i++) {
			String[] kv = fields[i].trim().split("\\s+");
			colTypes[i] = CsvUtil.stringToType(kv[1]);
		}
		return colTypes;
	}

	public static Row convertToRow(String data, Class <?>[] colClasses, String fieldDelim) {
		String[] fields = data.split(fieldDelim);
		if (fields.length != colClasses.length) {
			throw new IllegalArgumentException(
				"Unmatched number of fields: " + fields.length + " vs " + colClasses.length +
					" data: " + data);
		}
		Row row = new Row(fields.length);
		for (int i = 0; i < fields.length; i++) {
			if (colClasses[i].equals(String.class)) {
				row.setField(i, fields[i]);
			} else {
				row.setField(i, MLSession.gson.fromJson(fields[i], colClasses[i]));
			}
		}
		return row;
	}

	/**
	 * Converts the type name to Flink's TypeInformation.
	 * This is used to parse users' schemaStr
	 *
	 * @param type
	 * @return
	 */
	public static TypeInformation stringToType(String type) {
		if (type.compareToIgnoreCase("varchar") == 0 || type.compareToIgnoreCase("string") == 0) {
			return Types.STRING();
		} else if (type.compareToIgnoreCase("varbinary") == 0) {
			return BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
		} else if (type.compareToIgnoreCase("boolean") == 0) {
			return Types.BOOLEAN();
		} else if (type.compareToIgnoreCase("tinyint") == 0) {
			return Types.BYTE();
		} else if (type.compareToIgnoreCase("smallint") == 0) {
			return Types.SHORT();
		} else if (type.compareToIgnoreCase("int") == 0) {
			return Types.INT();
		} else if (type.compareToIgnoreCase("bigint") == 0 || type.compareToIgnoreCase("long") == 0) {
			return Types.LONG();
		} else if (type.compareToIgnoreCase("float") == 0) {
			return Types.FLOAT();
		} else if (type.compareToIgnoreCase("double") == 0) {
			return Types.DOUBLE();
		} else if (type.compareToIgnoreCase("decimal") == 0) {
			return Types.DECIMAL();
		} else if (type.compareToIgnoreCase("date") == 0) {
			return Types.SQL_DATE();
		} else if (type.compareToIgnoreCase("time") == 0) {
			return Types.SQL_TIME();
		} else if (type.compareToIgnoreCase("timestamp") == 0) {
			return Types.SQL_TIMESTAMP();
		} else {
			throw new RuntimeException("Not supported data type: " + type);
		}
	}

	public static String typeToString(TypeInformation type) {
		if (type == null) {
			return "null";
		}
		if (type.equals(Types.STRING())) {
			return "string";
		} else if (type.equals(Types.LONG())) {
			return "bigint";
		} else if (type.equals(Types.INT())) {
			return "int";
		} else if (type.equals(Types.DOUBLE())) {
			return "double";
		} else if (type.equals(Types.FLOAT())) {
			return "float";
		} else if (type.equals(Types.BOOLEAN())) {
			return "boolean";
		} else {
			throw new RuntimeException("Not supported data type: " + type.toString());
		}
	}

	/**
	 * Map the Flink's TypeInformation object to Java classes.
	 * The Flink's documents have a complete list of these mappings.
	 * https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/table/sql.html
	 *
	 * @param type
	 * @return
	 */
	public static Class typeToClass(TypeInformation <?> type) {
		return type.getTypeClass();
		//        if (type.equals(Types.STRING())) {
		//            return java.lang.String.class;
		//        } else if (type.equals(Types.BOOLEAN())) {
		//            return java.lang.Boolean.class;
		//        } else if (type.equals(Types.BYTE())) {
		//            return java.lang.Byte.class;
		//        } else if (type.equals(Types.SHORT())) {
		//            return java.lang.Short.class;
		//        } else if (type.equals(Types.INT())) {
		//            return java.lang.Integer.class;
		//        } else if (type.equals(Types.LONG())) {
		//            return java.lang.Long.class;
		//        } else if (type.equals(Types.FLOAT())) {
		//            return java.lang.Float.class;
		//        } else if (type.equals(Types.DOUBLE())) {
		//            return java.lang.Double.class;
		//        } else if (type.equals(Types.DECIMAL())) {
		//            return java.math.BigDecimal.class;
		//        } else if (type.equals(Types.SQL_DATE())) {
		//            return java.sql.Date.class;
		//        } else if (type.equals(Types.SQL_TIME())) {
		//            return java.sql.Time.class;
		//        } else if (type.equals(Types.SQL_TIMESTAMP())) {
		//            return java.sql.Timestamp.class;
		//        } else {
		//            throw new RuntimeException("Not supported data type: " + type.toString());
		//        }
	}

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	/**
	 * Parse from string the value of some data type.
	 */
	public static Object parse(Class <?> clazz, String valueStr) throws Exception {
		if (clazz.equals(String.class)) {
			return valueStr;
		} else if (clazz.equals(Boolean.class)) {
			return Boolean.valueOf(valueStr);
		} else if (clazz.equals(Byte.class)) {
			return Byte.valueOf(valueStr);
		} else if (clazz.equals(Short.class)) {
			return Short.valueOf(valueStr);
		} else if (clazz.equals(Integer.class)) {
			return Integer.valueOf(valueStr);
		} else if (clazz.equals(Long.class)) {
			return Long.valueOf(valueStr);
		} else if (clazz.equals(Float.class)) {
			return Float.valueOf(valueStr);
		} else if (clazz.equals(Double.class)) {
			return Double.valueOf(valueStr);
		} else if (clazz.equals(BigDecimal.class)) {
			return new BigDecimal(valueStr);
		} else if (clazz.equals(java.sql.Date.class)) {
			return java.sql.Date.valueOf(valueStr);
		} else if (clazz.equals(java.sql.Time.class)) {
			return java.sql.Time.valueOf(valueStr);
		} else if (clazz.equals(Timestamp.class)) {
			Date dt = sdf.parse(valueStr);
			return new Timestamp(dt.getTime());
		} else {
			throw new RuntimeException("Unsupported class: " + clazz);
		}
	}

	public static Object convertJsonObject(Class <?> clazz, Object object) throws Exception {
		if (object instanceof String) {
			return CsvUtil.parse(clazz, (String) object);
		} else if (object instanceof Double) {
			Double dvalue = (Double) object;
			if (clazz.equals(String.class)) {
				return dvalue.toString();
			} else if (clazz.equals(Byte.class)) {
				return dvalue.byteValue();
			} else if (clazz.equals(Short.class)) {
				return dvalue.shortValue();
			} else if (clazz.equals(Integer.class)) {
				return dvalue.intValue();
			} else if (clazz.equals(Long.class)) {
				return dvalue.longValue();
			} else if (clazz.equals(Float.class)) {
				return dvalue.floatValue();
			} else if (clazz.equals(Double.class)) {
				return dvalue;
			} else if (clazz.equals(BigDecimal.class)) {
				return new BigDecimal(dvalue);
			} else if (clazz.equals(java.sql.Date.class)) {
				return new java.sql.Date(dvalue.longValue());
			} else if (clazz.equals(java.sql.Time.class)) {
				return new java.sql.Time(dvalue.longValue());
			} else if (clazz.equals(Timestamp.class)) {
				return new Timestamp(dvalue.longValue());
			} else {
				throw new RuntimeException("Unsupported class: " + clazz);
			}
		} else {
			return CsvUtil.parse(clazz, String.valueOf(object));
		}
	}

	private static int extractEscape(String s, int pos, StringBuilder sbd) {
		if (s.charAt(pos) != '\\') {
			return 0;
		}

		pos++;

		if (pos >= s.length()) {
			return 0;
		}

		char c = s.charAt(pos);

		if (c >= '0' && c <= '7') {
			int digit = 1;
			int i;
			for (i = 0; i < 2; i++) {
				if (pos + 1 + i >= s.length()) {
					break;
				}

				if (s.charAt(pos + 1 + i) >= '0' && s.charAt(pos + 1 + i) <= '7') {
					digit++;
				} else {
					break;
				}
			}
			int n = Integer.valueOf(s.substring(pos, pos + digit), 8);
			sbd.append(Character.toChars(n));
			return digit + 1;
		} else if (c == 'u') { // unicode
			pos++;
			int digit = 0;
			for (int i = 0; i < 4; i++) {
				if (pos + i >= s.length()) {
					break;
				}
				char ch = s.charAt(pos + i);
				if ((ch >= '0' && ch <= '9') || ((ch >= 'a' && ch <= 'f')) || ((ch >= 'A' && ch <= 'F'))) {
					digit++;
				} else {
					break;
				}
			}

			if (digit == 0) {
				return 0;
			}

			int n = Integer.valueOf(s.substring(pos, pos + digit), 16);
			sbd.append(Character.toChars(n));
			return digit + 2;
		} else {
			switch (c) {
				case '\\':
					sbd.append('\\');
					return 2;
				case '\'':
					sbd.append('\'');
					return 2;
				case '\"':
					sbd.append('"');
					return 2;
				case 'r':
					sbd.append('\r');
					return 2;
				case 'f':
					sbd.append('\f');
					return 2;
				case 't':
					sbd.append('\t');
					return 2;
				case 'n':
					sbd.append('\n');
					return 2;
				case 'b':
					sbd.append('\b');
					return 2;
				default:
					return 0;
			}
		}
	}

	public static String unEscape(String s) {
		if (s == null) {
			return null;
		}

		if (s.length() == 0) {
			return s;
		}

		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < s.length(); ) {
			int flag = extractEscape(s, i, sbd);
			if (flag <= 0) {
				sbd.append(s.charAt(i));
				i++;
			} else {
				i += flag;
			}
		}

		return sbd.toString();
	}

	public static void unzipFile(String fn, String dir) {
		//Open the file
		try (ZipFile file = new ZipFile(fn)) {
			FileSystem fileSystem = FileSystems.getDefault();
			//Get file entries
			Enumeration <? extends ZipEntry> entries = file.entries();

			//We will unzip files in this folder
			String uncompressedDirectory = dir + File.separator;
			if (!new File(uncompressedDirectory).exists()) {
				Files.createDirectory(fileSystem.getPath(uncompressedDirectory));
			}

			//Iterate over entries
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				//If directory then create a new directory in uncompressed folder
				if (entry.isDirectory()) {
					System.out.println("Creating Directory:" + uncompressedDirectory + entry.getName());
					Files.createDirectories(fileSystem.getPath(uncompressedDirectory + entry.getName()));
				}
				//Else create the file
				else {
					InputStream is = file.getInputStream(entry);
					BufferedInputStream bis = new BufferedInputStream(is);
					String uncompressedFileName = uncompressedDirectory + entry.getName();
					Path uncompressedFilePath = fileSystem.getPath(uncompressedFileName);
					Files.createFile(uncompressedFilePath);
					FileOutputStream fileOutput = new FileOutputStream(uncompressedFileName);
					while (bis.available() > 0) {
						fileOutput.write(bis.read());
					}
					fileOutput.close();
					System.out.println("Written :" + entry.getName());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String downloadFile(String filePath, String dir) {
		try {
			HttpURLConnection connection;
			URL url = new URL(filePath);
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoInput(true);
			connection.setConnectTimeout(5000);
			connection.setReadTimeout(60000);
			connection.setRequestMethod("GET");
			connection.connect();

			int indexOf = filePath.lastIndexOf("/");
			String fn = dir + File.separator + filePath.substring(indexOf + 1);

			int read;
			final int buffSize = 64 * 1024;
			byte[] buffer = new byte[buffSize];
			InputStream in = connection.getInputStream();

			FileOutputStream fos = new FileOutputStream(fn);

			while ((read = in.read(buffer, 0, buffSize)) != -1) {
				fos.write(buffer, 0, read);
			}

			connection.disconnect();
			fos.close();

			System.out.println("Downloaded \"" + filePath + "\" to \"" + fn + "\"");
			return fn;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Fail to download file " + filePath);
		}
	}

	public static String downloadFile(String filePath, String dir, String targetFileName) {
		try {
			HttpURLConnection connection;
			URL url = new URL(filePath);
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoInput(true);
			connection.setConnectTimeout(5000);
			connection.setReadTimeout(60000);
			connection.setRequestMethod("GET");
			connection.connect();

			String fn = dir + File.separator + targetFileName;
			File file = new File(fn);
			file.deleteOnExit();

			int read;
			final int buffSize = 64 * 1024;
			byte[] buffer = new byte[buffSize];
			InputStream in = connection.getInputStream();

			FileOutputStream fos = new FileOutputStream(fn);

			while ((read = in.read(buffer, 0, buffSize)) != -1) {
				fos.write(buffer, 0, read);
			}

			connection.disconnect();
			fos.close();

			return fn;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Fail to download file " + filePath);
		}
	}

	public static String createLocalDirectory(String prefix) throws IOException {
		if (StringUtils.isNullOrWhitespaceOnly(prefix)) {
			throw new RuntimeException("prefix is empty");
		}

		String userDir = System.getProperty("user.dir");
		if (StringUtils.isNullOrWhitespaceOnly(userDir)) {
			throw new RuntimeException("user.dir is empty");
		}

		/**
		 * NOTE:
		 * In cupid, ${user.dir} is on apsara disk, while ${user.dir}/local is pangu disk.
		 * We should write temp files to ${user.dir}/local
		 */

		File localDir = new File(userDir, "local");
		if (!localDir.isDirectory()) {
			localDir = new File(userDir);
		}

		File tempDir = new File(localDir, prefix);
		if (tempDir.exists()) {
			throw new RuntimeException("directory already exists: " + tempDir.getName());
		}
		tempDir.deleteOnExit();
		if (!tempDir.mkdir()) {
			throw new IOException("Failed to create temp directory " + tempDir.getPath());
		}

		return tempDir.getPath();
	}

	public static void setSafeDeleteFileOnExit(final String pathName) {
		String userDir = System.getProperty("user.dir");
		if (StringUtils.isNullOrWhitespaceOnly(userDir)) {
			throw new RuntimeException("user.dir is empty");
		}

		File file = new File(pathName);
		if (!file.getAbsolutePath().startsWith(userDir)) {
			throw new RuntimeException("Trying to delete a file/dir outside of user directory.");
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					File file = new File(pathName);
					if (file.exists()) {
						System.out.println("exit deleting " + file.getName());
						if (file.isFile()) {
							file.delete();
						} else if (file.isDirectory()) {
							FileUtils.deleteDirectory(file);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	public static void safeDeleteFile(final String pathName) {
		String userDir = System.getProperty("user.dir");
		if (StringUtils.isNullOrWhitespaceOnly(userDir)) {
			throw new RuntimeException("user.dir is empty");
		}

		File file = new File(pathName);
		if (!file.getAbsolutePath().startsWith(userDir)) {
			throw new RuntimeException("Trying to delete a file/dir outside of user directory.");
		}

		try {
			if (file.exists()) {
				System.out.println("deleting " + file.getName());
				if (file.isFile()) {
					file.delete();
				} else if (file.isDirectory()) {
					FileUtils.deleteDirectory(file);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Fail to delete " + pathName);
		}
	}

	public static void main(String[] args) throws Exception {

		final String tmpDir = createLocalDirectory("abc");
		setSafeDeleteFileOnExit(tmpDir);

		InputStream in = CsvUtil.class.getResourceAsStream("/tsne/Makefile");
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String confFileName = tmpDir + "/Makefile";
		BufferedWriter writer = new BufferedWriter(new FileWriter(confFileName));

		String line;
		while ((line = reader.readLine()) != null) {
			writer.write(line);
		}

		writer.close();
		reader.close();
		in.close();

		final String tmpDir2 = createLocalDirectory("abcd");
		safeDeleteFile(tmpDir2);
	}
}
