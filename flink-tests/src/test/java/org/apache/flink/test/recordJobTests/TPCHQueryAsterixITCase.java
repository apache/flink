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


package org.apache.flink.test.recordJobTests;

import org.apache.flink.api.common.Plan;
import org.apache.flink.test.recordJobs.relational.TPCHQueryAsterix;
import org.apache.flink.test.util.RecordAPITestBase;

public class TPCHQueryAsterixITCase extends RecordAPITestBase {

	private String ordersPath;
	private String custPath;
	private String resultPath;

	private static final String ORDERS = 
		  "1|1|O|173665.47|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |\n"
		+ "2|6|O|46929.18|1996-12-01|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot|\n"
		+ "3|2|F|193846.25|1993-10-14|5-LOW|Clerk#000000955|0|sly final accounts boost. carefully regular ideas cajole carefully. depos|\n"
		+ "4|8|O|32151.78|1995-10-11|5-LOW|Clerk#000000124|0|sits. slyly regular warthogs cajole. regular, regular theodolites acro|\n"
		+ "5|8|F|144659.20|1994-07-30|5-LOW|Clerk#000000925|0|quickly. bold deposits sleep slyly. packages use slyly|\n"
		+ "6|1|F|58749.59|1992-02-21|4-NOT SPECIFIED|Clerk#000000058|0|ggle. special, final requests are against the furiously specia|\n"
		+ "7|4|O|252004.18|1996-01-10|2-HIGH|Clerk#000000470|0|ly special requests |\n"
		+ "32|9|O|208660.75|1995-07-16|2-HIGH|Clerk#000000616|0|ise blithely bold, regular requests. quickly unusual dep|\n"
		+ "33|8|F|163243.98|1993-10-27|3-MEDIUM|Clerk#000000409|0|uriously. furiously final request|\n"
		+ "34|10|O|58949.67|1998-07-21|3-MEDIUM|Clerk#000000223|0|ly final packages. fluffily final deposits wake blithely ideas. spe|\n"
		+ "35|2|O|253724.56|1995-10-23|4-NOT SPECIFIED|Clerk#000000259|0|zzle. carefully enticing deposits nag furio|\n"
		+ "36|9|O|68289.96|1995-11-03|1-URGENT|Clerk#000000358|0| quick packages are blithely. slyly silent accounts wake qu|\n"
		+ "37|1|F|206680.66|1992-06-03|3-MEDIUM|Clerk#000000456|0|kly regular pinto beans. carefully unusual waters cajole never|\n"
		+ "38|7|O|82500.05|1996-08-21|4-NOT SPECIFIED|Clerk#000000604|0|haggle blithely. furiously express ideas haggle blithely furiously regular re|\n"
		+ "39|1|O|341734.47|1996-09-20|3-MEDIUM|Clerk#000000659|0|ole express, ironic requests: ir|\n"
		+ "64|4|F|39414.99|1994-07-16|3-MEDIUM|Clerk#000000661|0|wake fluffily. sometimes ironic pinto beans about the dolphin|\n"
		+ "65|2|P|110643.60|1995-03-18|1-URGENT|Clerk#000000632|0|ular requests are blithely pending orbits-- even requests against the deposit|\n"
		+ "66|3|F|103740.67|1994-01-20|5-LOW|Clerk#000000743|0|y pending requests integrate|\n";

	private static final String CUSTOMERS = 
		"1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e|\n"+
		"2|Customer#000000002|XSTf4,NCwDVaWNe6tEgvwfmRchLXak|13|23-768-687-3665|121.65|AUTOMOBILE|l accounts. blithely ironic theodolites integrate boldly: caref|\n"+
		"3|Customer#000000003|MG9kdTD2WBHm|1|11-719-748-3364|7498.12|AUTOMOBILE| deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov|\n"+
		"4|Customer#000000004|XxVSJsLAGtn|4|14-128-190-5944|2866.83|MACHINERY| requests. final, regular ideas sleep final accou|\n"+
		"5|Customer#000000005|KvpyuHCplrB84WgAiGV6sYpZq7Tj|3|13-750-942-6364|794.47|HOUSEHOLD|n accounts will have to unwind. foxes cajole accor|\n"+
		"6|Customer#000000006|sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn|20|30-114-968-4951|7638.57|AUTOMOBILE|tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious|\n"+
		"7|Customer#000000007|TcGe5gaZNgVePxU5kRrvXBfkasDTea|18|28-190-982-9759|9561.95|AUTOMOBILE|ainst the ironic, express theodolites. express, even pinto beans among the exp|\n"+
		"8|Customer#000000008|I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5|17|27-147-574-9335|6819.74|BUILDING|among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide|\n"+
		"9|Customer#000000009|xKiAFTjUsCuxfeleNqefumTrjS|8|18-338-906-3675|8324.07|FURNITURE|r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl|\n"+
		"10|Customer#000000010|6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2|5|15-741-346-9870|2753.54|HOUSEHOLD|es regular deposits haggle. fur|\n";

	private static final String EXPECTED_RESULT = 
		"7|BUILDING\n" +
		"1|HOUSEHOLD\n" + 
		"6|AUTOMOBILE\n" + 
		"2|MACHINERY\n" +
		"2|FURNITURE\n";

	public TPCHQueryAsterixITCase(){
		setTaskManagerNumSlots(DOP);
	}


	@Override
	protected void preSubmit() throws Exception {
		ordersPath = createTempFile("orders", ORDERS);
		custPath = createTempFile("customers", CUSTOMERS);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		TPCHQueryAsterix tpchBench = new TPCHQueryAsterix();
		return tpchBench.getPlan(new Integer(DOP).toString(), ordersPath, custPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}
