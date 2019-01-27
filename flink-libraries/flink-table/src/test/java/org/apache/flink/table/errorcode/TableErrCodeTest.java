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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.util.TableTestBase;

import org.junit.Test;

/**
 * Test of {@link TableErrors} and {@link TableErrorCode}.
 */
public class TableErrCodeTest extends TableTestBase {

	@Test
	public void testTableErrCodeValidation() {
		TableErrors.validate(TableErrorCode.class);
	}

	@Test
	public void testUnclassifiedExp() {
		String errMsg = "SQL parse failed. unknown error occurring during validation";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120001");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testColumnNotFound() {
		String errMsg = "From line 76, column 11 to line 76, column 18: " +
			"Column 'weibo_id' not found in table 't1'";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120002");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testTableNotFound() {
		String errMsg = "Table 'a' not found";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120003");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testObjNotFound() {
		String errMsg = "Object 'crm_cdm' not found";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120004");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testUnknownIdentifiler() {
		String errMsg = "Unknown identifier '*'";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120005");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testArgNumberInvalid() {
		String errMsg = "Invalid number of arguments to function 'DATE_FORMAT'." +
			"Was expecting 2 arguments";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120006");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testArgTypeInvalid() {
		String errMsg = "Cannot apply 'AND' to arguments of type '<BOOLEAN> AND <VARCHAR(2000)>'" +
			". Supported form(s): '<BOOLEAN> AND <BOOLEAN>'";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120007");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testFuncSignatureNoMatch() {
		String errMsg = "Given parameters of function 'TSDIFFFunc' do not match any signature\n" +
			". Actual: (java.lang.String, java.lang.String, java.lang.String) \n" +
			"Expected: (java.lang.String, java.lang.String)";
		//String errMsg = "No match found for function signature CURRENT_DATE()";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120008");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testExprNotGrounded() {
		String errMsg = "Expression 'scene_id' is not being grouped";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120009");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testNonQueryExpr() {
		String errMsg = "SQL parse failed. Non-query expression encountered in illegal context";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120010");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testIllegalUseOfNull() {
		String errMsg = "Illegal use of 'NULL'";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120011");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

	@Test
	public void testUnknownDataType() {
		String errMsg = "Unknown datatype name 'string'";
		thrown().expect(ValidationException.class);
		thrown().expectMessage("SQL-00120012");
		throw new ValidationException(CalciteErrorClassifier.classify(errMsg));
	}

}
