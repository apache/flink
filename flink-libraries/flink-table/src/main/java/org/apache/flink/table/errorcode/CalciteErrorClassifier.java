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

import java.util.regex.Pattern;

/**
 * tools for classifying calcite errors, based on calcite error message.
 */
public class CalciteErrorClassifier {

	/**
	 * calcite error classifications.
	 */
	public enum CalciteErrTypes {

		/**
		 * column not found exception reported by calcite.
		 */
		columnNotFound(Pattern.compile("Column '.*' not found", Pattern.CASE_INSENSITIVE)),

		/**
		 * table not found exception reported by calcite.
		 */
		tableNotFound(Pattern.compile("Table '.*' not found", Pattern.CASE_INSENSITIVE)),

		/**
		 * object not found exception reported by calcite.
		 */
		objNotFound(Pattern.compile("Object '.*' not found", Pattern.CASE_INSENSITIVE)),

		/**
		 * unknown identifiler exception reported by calcite.
		 */
		unknownIdentifiler(Pattern.compile("Unknown identifier '.*'", Pattern.CASE_INSENSITIVE)),

		/**
		 * argument number invalid exception reported by calcite.
		 */
		argNumberInvalid(Pattern.compile("Invalid number of arguments to function '.*'",
			Pattern.CASE_INSENSITIVE)),

		/**
		 * argument type exception reported by calcite.
		 */
		argTypeInvalid(Pattern.compile("Cannot apply '.*' to arguments of type '.*'",
			Pattern.CASE_INSENSITIVE)),

		/**
		 * function signature not match exception reported by calcite.
		 */
		funcSignatureNoMatch(Pattern.compile("(No match found for function signature)|" +
			"(Given parameters of function '.*' do not match any signature)",
			Pattern.CASE_INSENSITIVE)),

		/**
		 * expression not grounded exception reported by calcite.
		 */
		exprNotGrounded(Pattern.compile("Expression '.*' is not being grouped",
			Pattern.CASE_INSENSITIVE)),

		/**
		 * non-query expression exception reported by calcite.
		 */
		nonQueryExpr(Pattern.compile("Non-query expression encountered in illegal context",
			Pattern.CASE_INSENSITIVE)),

		/**
		 * illegal use of NULL exception reported by calcite.
		 */
		illegalUseOfNull(Pattern.compile("Illegal use of 'NULL'", Pattern.CASE_INSENSITIVE)),

		/**
		 * unknown data type exception reported by calcite.
		 */
		unknownDataType(Pattern.compile("Unknown datatype name '.*'", Pattern.CASE_INSENSITIVE));

		private Pattern pattern;

		CalciteErrTypes(Pattern pattern) {
			this.pattern = pattern;
		}

		public Pattern getPattern() {
			return pattern;
		}
	}

	/**
	 * classify exception message by calcite, and get our own error codes based on classification result.
	 * @param expMsg
	 * @return
	 */
	public static String classify(String expMsg) {
		for (CalciteErrTypes type : CalciteErrTypes.values()) {
			if (type.getPattern().matcher(expMsg).find()) {
				switch(type) {
					case columnNotFound:
						return TableErrors.INST.sqlColumnNotFoundByCalcite(expMsg);
					case tableNotFound:
						return TableErrors.INST.sqlTableNotFoundByCalcite(expMsg);
					case objNotFound:
						return TableErrors.INST.sqlObjNotFoundByCalcite(expMsg);
					case unknownIdentifiler:
						return TableErrors.INST.sqlUnknownIdentifierByCalcite(expMsg);
					case argNumberInvalid:
						return TableErrors.INST.sqlArgNumberInvalidByCalcite(expMsg);
					case argTypeInvalid:
						return TableErrors.INST.sqlArgTypeInvalidByCalcite(expMsg);
					case funcSignatureNoMatch:
						return TableErrors.INST.sqlFuncSignatureNoMatchByCalcite(expMsg);
					case exprNotGrounded:
						return TableErrors.INST.sqlExprNotGroundedByCalcite(expMsg);
					case nonQueryExpr:
						return TableErrors.INST.sqlNonQueryExprByCalcite(expMsg);
					case illegalUseOfNull:
						return TableErrors.INST.sqlIllegalUseOfNullByCalcite(expMsg);
					case unknownDataType:
						return TableErrors.INST.sqlUnknownDataTypeByCalcite(expMsg);
					default:
						//do nothing
				}
			}
		}
		return TableErrors.INST.sqlUnclassifiedExpByCalcite(expMsg);
	}

}
