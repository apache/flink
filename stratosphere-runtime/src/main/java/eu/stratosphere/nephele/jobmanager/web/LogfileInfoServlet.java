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

package eu.stratosphere.nephele.jobmanager.web;

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

import eu.stratosphere.util.StringUtils;

public class LogfileInfoServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(LogfileInfoServlet.class);
	
	private File logDir;
	
	public LogfileInfoServlet(File logDir) {
		this.logDir = logDir;
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {
			if("stdout".equals(req.getParameter("get"))) {
				// Find current stdtout file
				for(File f : logDir.listFiles()) {
					// contains "jobmanager" ".log" and no number in the end ->needs improvement
					if( f.getName().equals("jobmanager-stdout.log") ||
							(f.getName().indexOf("jobmanager") != -1 && f.getName().indexOf(".out") != -1 && ! Character.isDigit(f.getName().charAt(f.getName().length() - 1) ))
							) {
						
						resp.setStatus(HttpServletResponse.SC_OK);
						resp.setContentType("text/plain ");
						writeFile(resp.getOutputStream(), f);
						break;
					}
				}
			}
			else {
				// Find current logfile
				for(File f : logDir.listFiles()) {
					// contains "jobmanager" ".log" and no number in the end ->needs improvement
					if( f.getName().equals("jobmanager-stderr.log") ||
							(f.getName().indexOf("jobmanager") != -1 && f.getName().indexOf(".log") != -1 && ! Character.isDigit(f.getName().charAt(f.getName().length() - 1) ))) {
						
						resp.setStatus(HttpServletResponse.SC_OK);
						resp.setContentType("text/plain ");
						writeFile(resp.getOutputStream(), f);
						break;
					}
					
				}
			}
		} catch (Throwable t) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print(t.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(t));
			}
		}
	}
	
	private static void writeFile(OutputStream out, File file) throws IOException {
		byte[] buf = new byte[4 * 1024]; // 4K buffer
		
		FileInputStream  is = null;
		try {
			is = new FileInputStream(file);
			
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
