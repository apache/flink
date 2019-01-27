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

package org.apache.flink.sql.parser.errorcode;

/**
 * error codes in flink table, and associated methods for call in corresponding scenario
 * Please note that a proxy instances is created for unnamed
 * class implementing this interface as static members in Class 'TableErrors'.
 *
 * <p>error codes have such elements as: code, cause, details and action.
 *    For example:
 *        SQL_001
 *        cause:   mismatched field(s)
 *        details: mismatched field [b: String] and [b1: Int]
 *        action:  re-check sql grammar
 *
 * <p>When adding new error code, please follow below steps:
 *     1. check existing error codes to see if there's already some that can cover the error
 *        scenario that you specify.
 *     2. If no existing code can meet your case, you can freely add one following
 *        these rules:
 *          I.  code format is:
 *                  [module short name] - [eight digit numbers]
 *
 *               <p>module short name is module name and up to now includes SQL, CLI, PAR, CON, STB, RUN.
 *               But here in this file, only PAR is used as module short name, as long as this file
 *               is for blink sql parser module.
 *
 *               <p>In the scope of blink sql parser module, eight digit numbers shall have such format:
 *               	 00          -  xx           -   xxxx
 *                  [always 0]     [sub module]      [specific error]
 *
 *               <p>Take an union error as a example (SQL-00060001):
 *                  00      -      00              -    0001
 *                                [sub module]          specific error
 *
 *               <p>Up to now, list of the sub module parts includes:
 *                  	00: others - non-classified parts
 *
 *               <p>---------------------- !!!NOTE!!! ----------------------
 *               	1. When adding new error code, you shall always add it to the tail in
 *               	   the section you choose. Take the example above, existing errors in
 *               	   this section is 3, and when you add a new one, you shall pick
 *               	   "PAR-00000001" as its error code.
 *					2. When some existing error code becomes obsolete because of code changes
 *					   or other reason, DO NOT delete it here, JUST MARK IT WITH comment such as
 *					   "obsoleted".
 *         II. you need to declare a function associated with this error code that can
 *               be called by developer when throwing exceptions.
 *               function name shall bear meaning in accordance with the error code's scenario.
 *               All these function's implementations are similar and logic are in
 *               invoke() specified in proxy inst creating method in Class "ErrorFactory".
 *
 * <p>For example, assuming the exiting error codes for section [others] in blink sql parser module
 *    is from PAR-00000001 to PAR-000000003, and you need to add a new one after
 *    figuring out that no existing code meets your requirement.
 *    And a new error code is added like this:
 *         \@ErrCode (
 *             codeId="PAR-00000004",
 *             cause="NPE exception",
 *             details="npe when invoking method '...', method param is...",
 *             action="ask developer to check code logic"
 *         )
 *         String parUnionNpeErrorExample();
 *   error code in other module shall follow the same rule and is added in their err code
 *   definition interface respectively.
 *
 *  <p>declared functions are integral parts of error code definitions.
 *    They shall be called by developer in error-occurring scenario, and return type is
 *    String, which is used by exception as error messages.
 *    A typical usage is like:
 *        ...
 *        throw new InvalidParameterException(parNpeErrorExample());
 *
 * <p>NOTE:
 *     a. Each module has their error code definition interface respectively.
 *        And as for now, module names include:
 *        SQL -- table/sql api,
 *        CLI -- blink job launcher,
 *        PAR -- blink sql parser
 *               (common parser logic, mainly for implementing additional DDL logic
 *                and is used by table/sql api),
 *        RUN -- runtime,
 *        STB -- state backend,
 *        CON -- connector
 *     b. error cause shall be brief and to the point.
 *        error detail message shall be more detailed and precise
 *        error action message shall be helpful. If none, just leave it blank.
 *        If too much to write it here, just leave a link to [url] where you can freely
 *        fill in detailed help message.
 *     c. Associated function for call by developers can have parameters when needed.
 */
public interface ParserErrorCode {

	@ErrorFactory.ErrCode(
		codeId = "PAR-00000001",
		cause = "sql parse context error:\n" +
			"{0}",
		details = "",
		action = "please re-check the sql statement according to descriptions above.\n" +
			"If you need more help, please contact customer support for this."
	)
	String parParseContextError(String moreInfo);

}
