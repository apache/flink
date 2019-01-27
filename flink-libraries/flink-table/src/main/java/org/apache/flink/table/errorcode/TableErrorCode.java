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

package org.apache.flink.table.errorcode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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
 *               <p>module short name is module name and up to now includes SQL, CLI, PAR, CON,
 *               STB, RUN.
 *               But here in this file, only SQL is used as module short name, as long as this file
 *               is for table/sql api module.
 *
 *               <p>In the scope of table/SQL module, eight digit numbers shall have such format:
 *               	 xx       -  xx                         -   xxxx
 *                  [phase]	    [semantic classification]      [specific error]
 *
 *               <p>Take an union error as a example (SQL-00060001):
 *                  00                       -  06      -    0001
 *                  QC(query compile phase)     union        specific error
 *
 *               <p>Up to now, list of the first two parts:
 *                  phase includes:
 *                  	00: QC - query compile
 *                  	01: QO - query optimization
 *                  	02: QE - query execution
 *                  semantic classification includes:
 *                  	00: others - non-classified parts
 *                  	01: table    - table related handling (table is the concept in table env)
 *                  	02: view     - view related handling
 *                  	03: group
 *                  	04: over
 *                  	05: join
 *                  	06: union
 *                  	07: sort
 *                  	08: topn
 *                  	09: retraction
 *                  	10: build-in func
 *                   	11: udx
 *                   	12: calcite - reclassified errors thrown by calcite
 *
 *               <p>---------------------- !!!NOTE!!! ----------------------
 *               	1. When adding new error code, you shall always add it to the tail in
 *               	   the section you choose. Take the example of union, existing errors in
 *               	   this section is 3, and when you add a new one, you shall pick
 *               	   "SQL-00060004" as its error code.
 *					2. When some existing error code becomes obsolete because of code changes
 *					   or other reason, DO NOT delete it here, JUST MARK IT WITH comment such as
 *					   "obsoleted".
 *         II. you need to declare a function associated with this error code that can
 *               be called by developer when throwing exceptions.
 *               function name shall bear meaning in accordance with the error code's scenario.
 *               All these function's implementations are similar and logic are in
 *               invoke() specified in proxy inst creating method in Class "ErrorFactory".
 *
 * <p>For example, assuming the exiting error codes for section union in table/sql api module
 *    is from SQL-00060001 to SQL-000600003, and you need to add a new one after
 *    figuring out that no existing code meets your requirement.
 *    And a new error code is added like this:
 *         \@ErrCode (
 *             codeId="SQL-00060004",
 *             cause="Union All: NPE exception",
 *             details="npe when invoking method '...', method param is...",
 *             action="ask developer to check code logic"
 *         )
 *         String sqlUnionNpeError();
 *   error code in other module shall follow the same rule and is added in their err code
 *   definition interface respectively.
 *
 *  <p>declared functions are integral parts of error code definitions.
 *    They shall be called by developer in error-occurring scenario, and return type is
 *    String, which is used by exception as error messages.
 *    A typical usage is like:
 *        ...
 *        throw new InvalidParameterException(sqlNpeErrorExample());
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
public interface TableErrorCode {
	/**
	 * err code id, cause, detailed message and action.
	 **/
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@interface ErrCode {
		String codeId();

		String cause();

		String details();

		String action();
	}


	/* ################################### QC ################################### */
	/** ----------------------------- others ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00000001",
		cause = "No output table have been created yet. " +
			"A program needs at least one output table that consumes data.",
		details = "",
		action = "Please create output table(s) for your program"
	)
	String sqlCompileNoSinkTblError();

	@ErrCode(
		codeId = "SQL-00000002",
		cause = "Internal error #1 in sql compile",
		details = "SinkNode required here",
		action = "Normally, this happens unlikely. Please contact customer support for this."
	)
	String sqlCompileSinkNodeRequired();

	@ErrCode(
		codeId = "SQL-00000003",
		cause = "There are both Distinct AggCall and Approximate Distinct AggCall \n" +
			"in one sql statement, it is not supported yet.",
		details = "",
		action = "Please choose one of them."
	)
	String sqlDistinctConflict();

	/** ----------------------------- table ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00010001",
		cause = "Name of target table must not be null or empty.",
		details = "Name of TableSink must not be null or empty.",
		action = "re-check target table definition in your sql"
	)
	String sqlInvalidSinkTblName();

	@ErrCode(
		codeId = "SQL-00010002",
		cause = "{0} Not a state table.",
		details = "",
		action = "This happens unlikely. Please contact customer support for this."
	)
	String sqlNotStateTableError(String name);

	@ErrCode(
		codeId = "SQL-00010003",
		cause = "No table was registered under the name {0}",
		details = "",
		action = "Please confirm if the table with that name is defined already.\n" +
			"Normally, this is mostly caused by typo.\n" +
			"If need more help, please contact customer support."
	)
	String sqlTableNotRegistered(String tableName);

	@ErrCode(
		codeId = "SQL-00010004",
		cause = "The table registered as {0} is not a TableSink." +
			"You can only emit query results to a registered TableSink.",
		details = "",
		action = "This happens unlikely. Please contact customer support for this."
	)
	String sqlNotTableSinkError(String tableName);

	@ErrCode(
		codeId = "SQL-00010005",
		cause = "Insert into: Query result and target table ''{0}'' field length not match:\n" +
			"Query Result: {1}\n" +
			"Target Table: {2}",
		details = "",
		action = "Please make sure the field count of the query result is the same with " +
			"the target table"
	)
	String sqlInsertIntoMismatchedFieldLen(String sinkName, String types1, String types2);

	@ErrCode(
		codeId = "SQL-00010006",
		cause = "Insert into: Query result and target table ''{0}'' field type(s) not match.\n" +
			"Query Result diff fields: {1}\n" +
			"Target Table diff fields: {2}",
		details = "",
		action = "Please make sure the field type(s) of the query result is the same with " +
			"the target table"
	)
	String sqlInsertIntoMismatchedFieldTypes(String sinkName, String types1, String types2);

	@ErrCode(
		codeId = "SQL-00010007",
		cause = "Could not create table ''{0}'' as source table",
		details = "{1}",
		action = "Please refer to details section for hint.\n" +
				"If it doesn't help, please contact customer support"
	)
	String sqlRegisterTableErrorAsSource(String tableName, String cause);

	@ErrCode(
		codeId = "SQL-00010008",
		cause = "Could not create table ''{0}'' as target table",
		details = "{1}",
		action = "Please refer to details section for hint.\n" +
				"If it doesn't help, please contact customer support"
	)
	String sqlRegisterTableErrorAsSink(String tableName, String cause);

	@ErrCode(
		codeId = "SQL-00010009",
		cause = "Could not create table ''{0}'' as dim table",
		details = "{1}",
		action = "Please refer to details section for hint.\n" +
				"If it doesn't help, please contact customer support"
	)
	String sqlRegisterTableErrorAsDim(String tableName, String cause);

	@ErrCode(
		codeId = "SQL-00010010",
		cause = "Failed to create schema for table ''{0}'':\n" +
			"{1} table does not support WATERMARK and PROCTIME().",
		details = "",
		action = "Please double check the sql statement and remove watermark definition from " +
				"table definition."
	)
	String sqlTableTypeNotSupportWaterMark(String tableName, String tableType);

	@ErrCode(
		codeId = "SQL-00010011",
		cause = "Failed to create schema for table ''{0}'':\n" +
			"{1} table do not support computed column.",
		details = "",
		action = "please double check the sql statement and remove computed column definition " +
			"from the table definition."
	)
	String sqlTableTypeNotSupportComputedCol(String tableName, String tableType);

	@ErrCode(
		codeId = "SQL-00010012",
		cause = "Table ''{0}'' Column ''{1}'': Unsupported column type: ''{2}''",
		details = "",
		action = "Please refer to the blink user manual for the currently supported column types"
	)
	String sqlUnSupportedColumnType(String tableName, String colName, String colType);

	/** ----------------------------- view ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00020001",
		cause = "Failed to register view ''{0}'' because " +
			"registering view table to environment failed.",
		details = "",
		action = "Normally this happens unlikely. Please contact customer support for this"
	)
	String sqlRegisterViewErrorDueToViewTblRegExp(String viewName);

	@ErrCode(
		codeId = "SQL-00020002",
		cause = "View ''{0}'' definition and input fields not match!\n" +
			"Def Fields:   {1}\n" +
			"Input Fields: {2}",
		details = "",
		action = "Please re-check create view statement."
	)
	String sqlRegisterViewErrorFieldsMismatch(String viewName, String fields1, String fields2);

	/** ----------------------------- group ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00030001",
		cause = "Group Window Agg: Retraction on windowed GroupBy aggregation is not supported yet. ",
		details = "",
		action = "please re-check sql grammar. \n" +
			"Note: Windowed GroupBy aggregation should not follow a" +
			"non-windowed GroupBy aggregation."
	)
	String sqlGroupWindowAggTranslateRetractNotSupported();

	@ErrCode(
		codeId = "SQL-00030002",
		cause = "Group Window Agg: Time attribute could not be found.",
		details = "",
		action = "Time attribute could not be found. This is a bug.\n" +
			"please contact customer support for this"
	)
	String sqlGroupWindowAggTranslateTimeAttrNotFound();

	@ErrCode(
		codeId = "SQL-00030003",
		cause = "{0} aggregate function does not support type: ''{1}''.",
		details = "",
		action = "Please re-check the data type."
	)
	String sqlAggFunctionDataTypeNotSupported(String function, String dataType);

	/** ----------------------------- over ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00040001",
		cause = "Over Agg: Unsupported use of OVER windows. {0}",
		details = "",
		action = "please re-check the over window statement"
	)
	String sqlOverAggInvalidUseOfOverWindow(String subCase);

	@ErrCode(
		codeId = "SQL-00040002",
		cause = "Over Agg: The window rank function without order by.",
		details = "",
		action = "please re-check the over window statement"
	)
	String sqlOverRankWithoutOrderByInvalid();

	/** ----------------------------- join ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00050001",
		cause = "Joins should have at least one equality condition.\n" +
			"{0}",
		details = "",
		action = "please re-check the join statement and make sure there's " +
			"equality condition for join."
	)
	String sqlJoinEqualConditionNotFound(String more);

	@ErrCode(
		codeId = "SQL-00050002",
		cause = "Join[type: {0}] requires an equality condition on table's key field.",
		details = "",
		action = "please re-check the join statement and make sure there's " +
			"equality condition on table's key field."
	)
	String sqlJoinRequireEqCondOnKey(String type);

	@ErrCode(
		codeId = "SQL-00050003",
		cause = "Dimension table require to define an primary key or unique key or index.",
		details = "",
		action = "please re-check your dim table definition and make sure there's " +
			"primary or unique key or index defined."
	)
	String sqlDimTableRequiresIndex();

	@ErrCode(
		codeId = "SQL-00050004",
		cause = "Join: Equality join predicate on incompatible types. " +
			"{0}",
		details = "",
		action = "please re-check the join statement."
	)
	String sqlJoinEqualJoinOnIncompatibleTypes(String more);

	@ErrCode(
		codeId = "SQL-00050005",
		cause = "Join[type: {0}] only support LEFT JOIN or INNER JOIN, but was {1}",
		details = "",
		action = "please re-check your join statement."
	)
	String sqlJoinTypeNotSupported(String type, String joinType);

	@ErrCode(
		codeId = "SQL-00050006",
		cause = "Stream to table join only support Row type returned dimension table, " +
			"but was {0}",
		details = "",
		action = "please modify your dim table definition accordingly."
	)
	String sqlStreamToTblJoinDimTypeNotSupported(String type);

	@ErrCode(
		codeId = "SQL-00050007",
		cause = "{0}",
		details = "",
		action = "please modify your temporal table definition accordingly."
	)
	String sqlJoinTemporalTableError(String more);

	@ErrCode(
		codeId = "SQL-00050008",
		cause = "Join Internal Error: {0}",
		details = "",
		action = "Normally, this happens unlikely. please contact customer support for this"
	)
	String sqlJoinInternalError(String moreInfo);

	@ErrCode(
		codeId = "SQL-00050009",
		cause = "Window Join: Windowed stream join does not support updates.",
		details = "",
		action = "please re-check window join statement according to description above"
	)
	String sqlWindowJoinUpdateNotSupported();

	@ErrCode(
		codeId = "SQL-00050010",
		cause = "Window Join: RowTime inner join between stream and stream is not supported yet.",
		details = "",
		action = "please re-check window join statement according to description above"
	)
	String sqlWindowJoinRowTimeInnerJoinBetweenStreamNotSupported();

	@ErrCode(
		codeId = "SQL-00050011",
		cause = "Window Join: {0} between stream and stream is not supported yet.",
		details = "",
		action = "please re-check window join statement according to description above"
	)
	String sqlWindowJoinBetweenStreamNotSupported(String joinType);

	@ErrCode(
			codeId = "SQL-00050012",
			cause = "Join Dimension table requires an equality condition on ALL of table's " +
				"primary key(s) or unique key(s) or index field(s).",
			details = "",
			action = "please re-check the join statement and make sure there's equality condition on table's index field."
	)
	String sqlDimJoinRequireEqCondOnIndex();

	/** ----------------------------- union ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00060001",
		cause = "Union All: Cannot union streams of different type length.\n" +
			"First:  {0}\n" +
			"Mismatch: {1}\n",
		details = "",
		action = "please re-check union all statement according to the description above"
	)
	String sqlUnionAllFieldsCntMismatch(String firstFields, String mismatchFields);

	@ErrCode(
		codeId = "SQL-00060002",
		cause = "Union All: Cannot union streams of different type(s).\n" +
			"First different type(s):  {0}\n" +
			"Mismatch different type(s): {1}\n",
		details = "",
		action = "please re-check union all statement according to the description above"
	)
	String sqlUnionAllFieldsTypeMismatch(String firstDiff, String mismatchDiff);

	/** ----------------------------- sort ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00070001",
		cause = "Sort: Primary sort order of a streaming table must be ascending on time.",
		details = "",
		action = "please re-check sort statement according to the description above"
	)
	String sqlSortOrderError();

	@ErrCode(
		codeId = "SQL-00070002",
		cause = "Sort: Streaming tables do not support sort with offset and fetch.",
		details = "",
		action = "please re-check sql grammar"
	)
	String sqlSortOffsetAndFetchNotSupported();

	@ErrCode(
		codeId = "SQL-00070003",
		cause = "Sort: Streaming tables do not support sort with offset.",
		details = "",
		action = "please re-check sort statement according to the description above"
	)
	String sqlSortOffsetNotSupported();

	@ErrCode(
		codeId = "SQL-00070004",
		cause = "Sort: Streaming tables do not support sort with fetch.",
		details = "",
		action = "please re-check sort statement according to the description above"
	)
	String sqlSortFetchNotSupported();

	@ErrCode(
		codeId = "SQL-00070005",
		cause = "Sort: Internal Error",
		details = "",
		action = "Normally, this happens unlikely. please contact customer support for this"
	)
	String sqlSortInternalError();

	/** ----------------------------- topn ----------------------------- .**/
	/** ----------------------------- retraction ----------------------------- .**/
	/** ----------------------------- build-in func ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00100001",
		cause = "{1} param error: {0}",
		details = "",
		action = "Please re-check param type(s) of the mentioned operator"
	)
	String sqlCodeGenOperatorParamError(String cause, String opName);

	@ErrCode(
		codeId = "SQL-00100002",
		cause = "Unsupported call: {0}",
		details = "",
		action = "This indicates that there's build-in function usage in sql statement that is, " +
				"however, unsupported now.\n" +
				"In some scenarios, this is caused by TYPO errors.\n" +
				"In other cases, If you think this function should be supported, " +
				"you can create an issue and start a discussion for it."
	)
	String sqlCodeGenUnsupportedCall(String opName);

	@ErrCode(
		codeId = "SQL-00100003",
		cause = "Unsupported call: {0}",
		details = "",
		action = "This call is unsupported now.\n" +
				"In some cases, if you were sure the function name is correct, " +
				"this is often caused by unmatched parameter(s)\n" +
				"In other cases, If you think this function should be supported, " +
				"you can create an issue and start a discussion for it."
	)
	String sqlCodeGenUnsupportedScalaFunc(String func);

	@ErrCode(
		codeId = "SQL-00100004",
		cause = "Expect an array or a map.",
		details = "",
		action = "This indicates that Array or Map is expected " +
				"but not present in your sql statement."
	)
	String sqlCodeGenItemOperatorError();

	/** ----------------------------- udx ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00110001",
		cause = "creating user defined function fails, functionName = {0}, className = {1}\n" +
			"{2}",
		details = "",
		action = "Normally, this is caused by not uploading and/or referring\n" +
			"jar file containing udf definitions. Please double check your job resource"
	)
	String sqlCreateUserDefinedFuncError(String funcName, String clsName, String moreInfo);

	@ErrCode(
		codeId = "SQL-00110002",
		cause = "registering user defined function failed, functionName = {0}, className = {1}\n" +
			"{2}",
		details = "",
		action = "Normally, this is caused by not implementing\n" +
			"udf correctly. Please double check your udf code"
	)
	String sqlRegisterUserDefinedFuncError(String funcName, String clsName, String moreInfo);

	@ErrCode(
		codeId = "SQL-00111000",
		cause = "Can't start python process and connect to it.\n",
		details = "{0}",
		action = "Normally, this is caused by not uploading and/or referring\n" +
			"python zip file containing udf definitions. Please double check your job resource"
	)
	String sqlPythonProcessError(String errMessage);

	@ErrCode(
		codeId = "SQL-00111001",
		cause = "Can't create socket and connect to python process.\n",
		details = "{0}",
		action = "Normally, this is caused by python process which was killed."
	)
	String sqlPythonCreateSocketError(String errMessage);

	@ErrCode(
		codeId = "SQL-00111002",
		cause = "Can't connect to Python process now for UDF {0} {1}\n",
		details = "{2}",
		action = "Normally, it was caused by an exception or the socket or python process" +
			"was closed."
	)
	String sqlPythonUDFSocketIOError(String funName, String pyModule, String pyErr);

	@ErrCode(
		codeId = "SQL-00111003",
		cause = "Error was raised in python UDF {0} {1}: \n" ,
		details = "{2}",
		action = "Normally, it was caused by python UDF error, " +
			"please double check your python code"
	)
	String sqlPythonUDFRunTimeError(String funName, String pyModule, String pyErr);

	/** ----------------------------- calcite ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-00120001",
		cause = "{0}",
		details = "",
		action = "Please see descriptions above. " +
				"If it doesn't help, please contact customer support for this."
	)
	String sqlUnclassifiedExpByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120002",
		cause = "{0}",
		details = "",
		action = "Might use undefined column(s) in sql statement? Please double-check relevant " +
				"table definitions"
	)
	String sqlColumnNotFoundByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120003",
		cause = "{0}",
		details = "",
		action = "Might refer undefined table(s) in sql statement? " +
				"Please make sure table(s) in question are already defined."
	)
	String sqlTableNotFoundByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120004",
		cause = "{0}",
		details = "",
		action = "It indicates that there's unidentified item in your sql statement. " +
				"Might be TYPO, might be other stuff that's not defined or supported. \n" +
				"Anyway, please double-check sql statement following hints given above"
	)
	String sqlObjNotFoundByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120005",
		cause = "{0}",
		details = "",
		action = "It indicates there exists unknown identifier in sql statement. " +
				"Might be TYPO, or undefined item, or unsupported semantic. \n" +
				"Anyway, please double-check sql statement following hints given above"
	)
	String sqlUnknownIdentifierByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120006",
		cause = "{0}",
		details = "",
		action = "Please re-check your sql statement as well as function definition and " +
				"make sure argument number are the same in actual call and definition"
	)
	String sqlArgNumberInvalidByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120007",
		cause = "{0}",
		details = "",
		action = "Please re-check your sql statement as well as function definition and " +
				"make sure argument type(s) are the same in actual call and definition"
	)
	String sqlArgTypeInvalidByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120008",
		cause = "{0}",
		details = "",
		action = "Please re-check your sql statement as well as function definition and " +
				"make sure function parameters are the same in actual call and definition"
	)
	String sqlFuncSignatureNoMatchByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120009",
		cause = "{0}",
		details = "",
		action = "Please re-check sql statement according to the description above"
	)
	String sqlExprNotGroundedByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120010",
		cause = "{0}",
		details = "",
		action = "It indicates your sql statement contains no query expression. \n" +
				"A valid sql for Blink Job must contains query expression."
	)
	String sqlNonQueryExprByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120011",
		cause = "{0}",
		details = "",
		action = "It indicates null is used in places that doesn't allow it to appear in " +
				"your sql statement"
	)
	String sqlIllegalUseOfNullByCalcite(String cause);

	@ErrCode(
		codeId = "SQL-00120012",
		cause = "{0}",
		details = "",
		action = "Might indicates unsupported dataType is present in your table definition. " +
				"Please note that it might also be caused by TYPO."
	)
	String sqlUnknownDataTypeByCalcite(String cause);

	/* ################################### QO ################################### */
	/** ----------------------------- others ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-01000001",
		cause = "Sql optimization: Cannot generate a valid execution plan for the given" +
			" query: \n {0} \n",
		details = "{1}",
		action = "Please check the documentation for the set of currently supported SQL features"
	)
	String sqlVolcanoOptimizeError(String query, String moreInfo);

	@ErrCode(
		codeId = "SQL-01000002",
		cause = "Sql optimization: Cannot generate a valid execution plan for the given" +
			" query: \n {0} \n This exception indicates that the query uses an unsupported SQL feature.",
		details = "{1}",
		action = "Please check the documentation for the set of currently supported SQL features"
	)
	String sqlVolcanoOptimizeUnsupportedSQLFeature(String query, String moreInfo);

	@ErrCode(
		codeId = "SQL-01000003",
		cause = "sql optimization: Assertion error",
		details = "{0}",
		action = "please contact customer support for this"
	)
	String sqlVolcanoOptimizeAssertionExp(String moreInfo);

	/* ################################### QE ################################### */
	/** ----------------------------- others ----------------------------- .**/
	/** ----------------------------- table ----------------------------- .**/
	/** ----------------------------- view ----------------------------- .**/

	/** ----------------------------- group ----------------------------- .**/
	@ErrCode(
		codeId = "SQL-02030001",
		cause = "{0} aggregate execution does not support type: ''{1}''.",
		details = "",
		action = "Please re-check the data type."
	)
	String sqlAggExecDataTypeNotSupported(String function, String dataType);
}
