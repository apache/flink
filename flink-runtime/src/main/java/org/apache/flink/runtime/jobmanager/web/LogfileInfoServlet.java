/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.flink.util.StringUtils;

import com.google.common.base.Preconditions;

public class LogfileInfoServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(LogfileInfoServlet.class);

	private File[] logDirs;


	public LogfileInfoServlet(File[] logDirs) {
		Preconditions.checkNotNull(logDirs, "The given log files are null.");
		this.logDirs = logDirs;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {
			if("stdout".equals(req.getParameter("get"))) {
				// Find current stdout file
				sendFile("jobmanager-stdout.log", resp);
			}
			else {
				// Find current logfile
				sendFile("jobmanager-log4j.log", resp);
			}
		} catch (Throwable t) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print("Error opening log files':"+t.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(t));
			}
		}
	}

	private void sendFile(String fileName, HttpServletResponse resp) throws IOException {
		for(File logDir: logDirs) {
			for(File f : logDir.listFiles()) {
				// contains "jobmanager" ".log" and no number in the end ->needs improvement
				if( f.getName().equals(fileName) /*||
						(f.getName().indexOf("jobmanager") != -1 && f.getName().indexOf(".log") != -1 && ! Character.isDigit(f.getName().charAt(f.getName().length() - 1) )) */
						) {

					resp.setStatus(HttpServletResponse.SC_OK);
					resp.setContentType("text/plain");
					writeFile(resp.getOutputStream(), f);
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
