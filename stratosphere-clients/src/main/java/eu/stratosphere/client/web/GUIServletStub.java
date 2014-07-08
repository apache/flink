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

package eu.stratosphere.client.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public abstract class GUIServletStub extends HttpServlet {
	/**
	 * The content type for plain textual data.
	 */
	public static final String CONTENT_TYPE_PLAIN = "text/plain;charset=utf-8";

	/**
	 * The content type for HTML data.
	 */
	public static final String CONTENT_TYPE_HTML = "text/html;charset=utf-8";

	// ------------------------------------------------------------------------

	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = -7992677786569004843L;

	/**
	 * The references to CSS files, to be included in the header.
	 */
	private List<String> cssFiles;

	/**
	 * The javascript files to be included.
	 */
	private List<String> jsFiles;

	/**
	 * The title of the website.
	 */
	private String title;

	/**
	 * The constructor to be invoked by subclasses.
	 * 
	 * @param title
	 *        The title of the page, to be entered into the headers title tag.
	 */
	public GUIServletStub(String title) {
		this.title = title;
		this.cssFiles = new ArrayList<String>();
		this.jsFiles = new ArrayList<String>();
	}

	/**
	 * Adds a stylesheet file to be included in the header.
	 * 
	 * @param file
	 *        The file to be included.
	 */
	public void addStyleSheet(String file) {
		cssFiles.add(file);
	}

	/**
	 * Adds a javascript file to be included in the header.
	 * 
	 * @param file
	 *        The file to be included.
	 */
	public void addJavascriptFile(String file) {
		jsFiles.add(file);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// response setup
		resp.setContentType("text/html;charset=utf-8");
		resp.setStatus(HttpServletResponse.SC_OK);

		PrintWriter writer = resp.getWriter();

		// print the header
		writer
			.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"\n        \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
		writer.println("<html>");
		writer.println("<head>");
		writer.print("  <title>");
		writer.print(title);
		writer.println("</title>");
		writer.println("  <meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\" />");

		// print all the stylesheets
		writer.println("  <link rel=\"stylesheet\" type=\"text/css\" href=\"css/nephelefrontend.css\" />");

		for (int i = 0; i < cssFiles.size(); i++) {
			writer.print("  <link rel=\"stylesheet\" type=\"text/css\" href=\"");
			writer.print(cssFiles.get(i));
			writer.println("\" />");
		}

		// print all the included javascript files

		for (int i = 0; i < jsFiles.size(); i++) {
			writer.print("  <script type=\"text/javascript\" src=\"");
			writer.print(jsFiles.get(i));
			writer.println("\"></script>");
		}

		// write the other scripts and style definitions

		// write the header
		writer.println("<body>");
		writer.println("  <div class=\"mainHeading\">");
		writer
			.println("    <h1 style=\"margin-top:0\"><img src=\"img/StratosphereLogo.png\" width=\"326\" height=\"100\" alt=\"Stratosphere Logo\" align=\"middle\"/>Stratosphere Query Interface</h1>");
		writer.println("  </div>");

		@SuppressWarnings("unchecked")
		Map<String, String[]> m = (Map<String, String[]>) req.getParameterMap();

		// let the content be printed by the child class
		printPage(writer, m, req);

		// print the footer
		writer.println("</body>");
		writer.println("</html>");

	}

	/**
	 * This method must be overridden by the subclass. It will be called to print the contents
	 * of the page.
	 * 
	 * @param writer
	 *        The <tt>PrintWriter</tt> to print the content to.
	 * @param parameters
	 *        The map containing all parameters mapped to their values.
	 * @throws IOException
	 *         If the request processing failed due to an I/O problem.
	 */
	public abstract void printPage(PrintWriter writer, Map<String, String[]> parameters, HttpServletRequest req) throws IOException;
}
