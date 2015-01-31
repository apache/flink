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

package org.apache.flink.runtime.jobmanager.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.util.StringUtils;

public class LogfileInfoServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	/**
	 * The log for this class.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(LogfileInfoServlet.class);

	private File[] logDirs;


	public LogfileInfoServlet(File[] logDirs) {
		if(logDirs == null){
			throw new NullPointerException("The given log files are null.");
		}
		this.logDirs = logDirs;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {
			if("stdout".equals(req.getParameter("get"))) {
				// Find current stdout file
				sendFile(".*jobmanager-[^\\.]*\\.out", resp);
			}
			else {
				// Find current logfile
				sendFile(".*jobmanager-[^\\.]*\\.log", resp);
			}
		} catch (Throwable t) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print("Error opening log files':"+t.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(t));
			}
		}
	}

	private void sendFile(String fileNamePattern, HttpServletResponse resp) throws IOException {
		for(File logDir: logDirs) {
			if(logDir == null) {
				continue;
			}
			File[] files = logDir.listFiles();
			if(files == null) {
				resp.setStatus(HttpServletResponse.SC_OK);
				resp.setContentType("text/plain");
				resp.getOutputStream().write(("The specified log directory '"+logDir+"' is empty").getBytes());
			} else {
				for (File f : files) {
					// contains "jobmanager" ".log" and no number in the end ->needs improvement
					if (f.getName().matches(fileNamePattern)) {
						resp.setStatus(HttpServletResponse.SC_OK);
						resp.setContentType("text/plain");
						writeFile(resp.getOutputStream(), f);
					}
				}
			}
		}
	}
	private static void writeFile(OutputStream out, File file) throws IOException {
		byte[] buf = new byte[4 * 1024]; // 4K buffer

		FileInputStream  is = null;
		try {
			is = new FileInputStream(file);
			out.write(("==== FILE: "+file.toString()+" ====\n").getBytes());
			int bytesRead;
			while ((bytesRead = is.read(buf)) != -1) {
				out.write(buf, 0, bytesRead);
			}
		} finally {
			if (is != null) {
				is.close();
			}
		}
	}

}
