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

package eu.stratosphere.client.web;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

/**
 * A servlet that accepts uploads of pact programs, returns a listing of the
 * directory containing the job jars, and deleting these jars.
 */
public class JobsServlet extends HttpServlet {
	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = -1373210261957434273L;

	// ------------------------------------------------------------------------

	private static final String ACTION_PARAM_NAME = "action";

	private static final String ACTION_LIST_VALUE = "list";

	private static final String ACTION_DELETE_VALUE = "delete";

	private static final String FILENAME_PARAM_NAME = "filename";

	private static final String CONTENT_TYPE_PLAIN = "text/plain";

	private static final Comparator<File> FILE_SORTER = new Comparator<File>() {
		@Override
		public int compare(File o1, File o2) {
			return o1.getName().compareTo(o2.getName());
		}

	};

	// ------------------------------------------------------------------------

	/**
	 * The file object for the directory for temporary files.
	 */
	private final File tmpDir;

	/**
	 * The file object for the directory for the submitted jars.
	 */
	private final File destinationDir;

	/**
	 * The name of the file to redirect to after the upload.
	 */
	private final String targetPage;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new <tt>JobServlet</tt>, configured with the given directories
	 * and the given result page.
	 * 
	 * @param jobsDir
	 *        The directory to store uploaded jobs in.
	 * @param tmpDir
	 *        The directory for temporary files.
	 * @param targetPage
	 *        The page to redirect to after a successful upload.
	 */
	public JobsServlet(File jobsDir, File tmpDir, String targetPage) {
		this.tmpDir = tmpDir;
		this.destinationDir = jobsDir;
		this.targetPage = targetPage;
	}

	// ------------------------------------------------------------------------

	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		if (!(tmpDir.isDirectory() && tmpDir.canWrite())) {
			throw new ServletException(tmpDir.getAbsolutePath() + " is not a writable directory");
		}

		if (!(destinationDir.isDirectory() && destinationDir.canWrite())) {
			throw new ServletException(destinationDir.getAbsolutePath() + " is not a writable directory");
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String action = req.getParameter(ACTION_PARAM_NAME);

		if (action.equals(ACTION_LIST_VALUE)) {
			GregorianCalendar cal = new GregorianCalendar();

			File[] files = destinationDir.listFiles();
			Arrays.<File> sort(files, FILE_SORTER);

			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setContentType(CONTENT_TYPE_PLAIN);

			PrintWriter writer = resp.getWriter();
			for (int i = 0; i < files.length; i++) {
				if (!files[i].getName().endsWith(".jar")) {
					continue;
				}

				cal.setTimeInMillis(files[i].lastModified());
				writer.println(files[i].getName() + '\t' + (cal.get(GregorianCalendar.MONTH) + 1) + '/'
					+ cal.get(GregorianCalendar.DAY_OF_MONTH) + '/' + cal.get(GregorianCalendar.YEAR) + ' '
					+ cal.get(GregorianCalendar.HOUR_OF_DAY) + ':' + cal.get(GregorianCalendar.MINUTE) + ':'
					+ cal.get(GregorianCalendar.SECOND));
			}
		} else if (action.equals(ACTION_DELETE_VALUE)) {
			String filename = req.getParameter(FILENAME_PARAM_NAME);

			if (filename == null || filename.length() == 0) {
				resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			} else {
				File f = new File(destinationDir, filename);
				if (!f.exists() || f.isDirectory()) {
					resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
				}
				f.delete();
				resp.setStatus(HttpServletResponse.SC_OK);
			}
		} else {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		}
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// check, if we are doing the right request
		if (!ServletFileUpload.isMultipartContent(req)) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		// create the disk file factory, limiting the file size to 20 MB
		DiskFileItemFactory fileItemFactory = new DiskFileItemFactory();
		fileItemFactory.setSizeThreshold(20 * 1024 * 1024); // 20 MB
		fileItemFactory.setRepository(tmpDir);

		String filename = null;

		// parse the request
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
		try {
			@SuppressWarnings("unchecked")
			Iterator<FileItem> itr = ((List<FileItem>) uploadHandler.parseRequest(req)).iterator();

			// go over the form fields and look for our file
			while (itr.hasNext()) {
				FileItem item = itr.next();
				if (!item.isFormField()) {
					if (item.getFieldName().equals("upload_jar_file")) {

						// found the file, store it to the specified location
						filename = item.getName();
						File file = new File(destinationDir, filename);
						item.write(file);
						break;
					}
				}
			}
		} catch (FileUploadException ex) {
			resp.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Invalid Fileupload.");
			return;
		} catch (Exception ex) {
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
				"An unknown error occurred during the file upload.");
			return;
		}

		// write the okay message
		resp.sendRedirect(targetPage);
	}

}
