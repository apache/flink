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

package eu.stratosphere.test.exampleRecordPrograms;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.test.testPrograms.tpch4.TPCHQuery4;
import eu.stratosphere.test.util.TestBase2;

public class TPCHQuery4ITCase extends TestBase2 {
	
	private String ordersPath;
	private String lineitemsPath;
	private String resultPath ;

	private final String ORDERS = "1|36901|O|173665.47|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |\n"
		+ "2|78002|O|46929.18|1996-12-01|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot|\n"
		+ "3|123314|F|193846.25|1993-10-14|5-LOW|Clerk#000000955|0|sly final accounts boost. carefully regular ideas cajole carefully. depos|\n"
		+ "4|136777|O|32151.78|1995-10-11|5-LOW|Clerk#000000124|0|sits. slyly regular warthogs cajole. regular, regular theodolites acro|\n"
		+ "5|44485|F|144659.20|1994-07-30|5-LOW|Clerk#000000925|0|quickly. bold deposits sleep slyly. packages use slyly|\n"
		+ "6|55624|F|58749.59|1995-02-21|4-NOT SPECIFIED|Clerk#000000058|0|ggle. special, final requests are against the furiously specia|\n"
		+ "7|39136|O|252004.18|1996-01-10|2-HIGH|Clerk#000000470|0|ly special requests |\n"
		+ "32|130057|O|208660.75|1995-07-16|2-HIGH|Clerk#000000616|0|ise blithely bold, regular requests. quickly unusual dep|\n"
		+ "33|66958|F|163243.98|1993-10-27|3-MEDIUM|Clerk#000000409|0|uriously. furiously final request|\n"
		+ "34|61001|O|58949.67|1998-07-21|3-MEDIUM|Clerk#000000223|0|ly final packages. fluffily final deposits wake blithely ideas. spe|\n"
		+ "35|127588|O|253724.56|1995-01-23|4-NOT SPECIFIED|Clerk#000000259|0|zzle. carefully enticing deposits nag furio|\n"
		+ "36|115252|O|68289.96|1995-11-03|1-URGENT|Clerk#000000358|0| quick packages are blithely. slyly silent accounts wake qu|\n"
		+ "37|86116|F|206680.66|1995-02-03|3-MEDIUM|Clerk#000000456|0|kly regular pinto beans. carefully unusual waters cajole never|\n"
		+ "38|124828|O|82500.05|1996-08-21|4-NOT SPECIFIED|Clerk#000000604|0|haggle blithely. furiously express ideas haggle blithely furiously regular re|\n"
		+ "39|81763|O|341734.47|1996-09-20|3-MEDIUM|Clerk#000000659|0|ole express, ironic requests: ir|\n"
		+ "64|32113|F|39414.99|1994-07-16|3-MEDIUM|Clerk#000000661|0|wake fluffily. sometimes ironic pinto beans about the dolphin|\n"
		+ "65|16252|P|110643.60|1995-03-18|1-URGENT|Clerk#000000632|0|ular requests are blithely pending orbits-- even requests against the deposit|\n"
		+ "66|129200|F|103740.67|1994-01-20|5-LOW|Clerk#000000743|0|y pending requests integrate|\n";

	private static final String LINEITEMS = "1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|\n"
		+ "1|67310|7311|2|36|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |\n"
		+ "1|63700|3701|3|8|13309.60|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep|\n"
		+ "1|2132|4633|4|28|28955.64|0.09|0.06|N|O|1996-04-21|1996-03-30|1996-05-16|NONE|AIR|lites. fluffily even de|\n"
		+ "1|24027|1534|5|24|22824.48|0.10|0.04|N|O|1996-03-30|1996-03-14|1996-04-01|NONE|FOB| pending foxes. slyly re|\n"
		+ "1|15635|638|6|32|49620.16|0.07|0.02|N|O|1996-01-30|1996-02-07|1996-02-03|DELIVER IN PERSON|MAIL|arefully slyly ex|\n"
		+ "2|106170|1191|1|38|44694.46|0.00|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|ven requests. deposits breach a|\n"
		+ "3|4297|1798|1|45|54058.05|0.06|0.00|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco|\n"
		+ "3|19036|6540|2|49|46796.47|0.10|0.00|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve|\n"
		+ "3|128449|3474|3|27|39890.88|0.06|0.07|A|F|1994-01-16|1993-11-22|1994-01-23|DELIVER IN PERSON|SHIP|nal foxes wake. |\n"
		+ "3|29380|1883|4|2|2618.76|0.01|0.06|A|F|1993-12-04|1994-01-07|1994-01-01|NONE|TRUCK|y. fluffily pending d|\n"
		+ "3|183095|650|5|28|32986.52|0.04|0.00|R|F|1993-12-14|1994-01-10|1994-01-01|TAKE BACK RETURN|FOB|ages nag slyly pending|\n"
		+ "3|62143|9662|6|26|28733.64|0.10|0.02|A|F|1993-10-29|1993-12-18|1993-11-04|TAKE BACK RETURN|RAIL|ges sleep after the caref|\n"
		+ "4|88035|5560|1|30|30690.90|0.03|0.08|N|O|1996-01-10|1995-12-14|1996-01-18|DELIVER IN PERSON|REG AIR|- quickly regular packages sleep. idly|\n"
		+ "5|108570|8571|1|15|23678.55|0.02|0.04|R|F|1994-10-31|1994-08-31|1994-11-20|NONE|AIR|ts wake furiously |\n"
		+ "5|123927|3928|2|26|50723.92|0.07|0.08|R|F|1994-10-16|1994-09-25|1994-10-19|NONE|FOB|sts use slyly quickly special instruc|\n"
		+ "5|37531|35|3|50|73426.50|0.08|0.03|A|F|1994-08-08|1994-10-13|1994-08-26|DELIVER IN PERSON|AIR|eodolites. fluffily unusual|\n"
		+ "6|139636|2150|1|37|61998.31|0.08|0.03|A|F|1995-04-27|1995-05-15|1995-05-20|TAKE BACK RETURN|TRUCK|p furiously special foxes|\n"
		+ "7|182052|9607|1|12|13608.60|0.07|0.03|N|O|1996-05-07|1996-03-13|1996-06-03|TAKE BACK RETURN|FOB|ss pinto beans wake against th|\n"
		+ "7|145243|7758|2|9|11594.16|0.08|0.08|N|O|1996-02-01|1996-03-02|1996-02-19|TAKE BACK RETURN|SHIP|es. instructions|\n"
		+ "7|94780|9799|3|46|81639.88|0.10|0.07|N|O|1996-01-15|1996-03-27|1996-02-03|COLLECT COD|MAIL| unusual reques|\n"
		+ "7|163073|3074|4|28|31809.96|0.03|0.04|N|O|1996-03-21|1996-04-08|1996-04-20|NONE|FOB|. slyly special requests haggl|\n"
		+ "7|151894|9440|5|38|73943.82|0.08|0.01|N|O|1996-02-11|1996-02-24|1996-02-18|DELIVER IN PERSON|TRUCK|ns haggle carefully ironic deposits. bl|\n"
		+ "7|79251|1759|6|35|43058.75|0.06|0.03|N|O|1996-01-16|1996-02-23|1996-01-22|TAKE BACK RETURN|FOB|jole. excuses wake carefully alongside of |\n"
		+ "7|157238|2269|7|5|6476.15|0.04|0.02|N|O|1996-02-10|1996-03-26|1996-02-13|NONE|FOB|ithely regula|\n"
		+ "32|82704|7721|1|28|47227.60|0.05|0.08|N|O|1995-10-23|1995-08-27|1995-10-26|TAKE BACK RETURN|TRUCK|sleep quickly. req|\n"
		+ "32|197921|441|2|32|64605.44|0.02|0.00|N|O|1995-08-14|1995-10-07|1995-08-27|COLLECT COD|AIR|lithely regular deposits. fluffily |\n"
		+ "32|44161|6666|3|2|2210.32|0.09|0.02|N|O|1995-08-07|1995-10-07|1995-08-23|DELIVER IN PERSON|AIR| express accounts wake according to the|\n"
		+ "32|2743|7744|4|4|6582.96|0.09|0.03|N|O|1995-08-04|1995-10-01|1995-09-03|NONE|REG AIR|e slyly final pac|\n"
		+ "32|85811|8320|5|44|79059.64|0.05|0.06|N|O|1995-08-28|1995-08-20|1995-09-14|DELIVER IN PERSON|AIR|symptotes nag according to the ironic depo|\n"
		+ "32|11615|4117|6|6|9159.66|0.04|0.03|N|O|1995-07-21|1995-09-23|1995-07-25|COLLECT COD|RAIL| gifts cajole carefully.|\n"
		+ "33|61336|8855|1|31|40217.23|0.09|0.04|A|F|1993-10-29|1993-12-19|1993-11-08|COLLECT COD|TRUCK|ng to the furiously ironic package|\n"
		+ "33|60519|5532|2|32|47344.32|0.02|0.05|A|F|1993-12-09|1994-01-04|1993-12-28|COLLECT COD|MAIL|gular theodolites|\n"
		+ "33|137469|9983|3|5|7532.30|0.05|0.03|A|F|1993-12-09|1993-12-25|1993-12-23|TAKE BACK RETURN|AIR|. stealthily bold exc|\n"
		+ "33|33918|3919|4|41|75928.31|0.09|0.00|R|F|1993-11-09|1994-01-24|1993-11-11|TAKE BACK RETURN|MAIL|unusual packages doubt caref|\n"
		+ "34|88362|871|1|13|17554.68|0.00|0.07|N|O|1998-10-23|1998-09-14|1998-11-06|NONE|REG AIR|nic accounts. deposits are alon|\n"
		+ "34|89414|1923|2|22|30875.02|0.08|0.06|N|O|1998-10-09|1998-10-16|1998-10-12|NONE|FOB|thely slyly p|\n"
		+ "34|169544|4577|3|6|9681.24|0.02|0.06|N|O|1998-10-30|1998-09-20|1998-11-05|NONE|FOB|ar foxes sleep |\n"
		+ "35|450|2951|1|24|32410.80|0.02|0.00|N|O|1995-02-21|1995-01-03|1995-03-18|TAKE BACK RETURN|FOB|, regular tithe|\n"
		+ "35|161940|4457|2|34|68065.96|0.06|0.08|N|O|1995-01-22|1995-01-06|1995-01-27|DELIVER IN PERSON|RAIL|s are carefully against the f|\n"
		+ "35|120896|8433|3|7|13418.23|0.06|0.04|N|O|1995-01-19|1995-12-22|1995-01-29|NONE|MAIL| the carefully regular |\n"
		+ "35|85175|7684|4|25|29004.25|0.06|0.05|N|O|1995-11-26|1995-12-25|1995-12-21|DELIVER IN PERSON|SHIP| quickly unti|\n"
		+ "35|119917|4940|5|34|65854.94|0.08|0.06|N|O|1995-11-08|1995-01-15|1995-11-26|COLLECT COD|MAIL|. silent, unusual deposits boost|\n"
		+ "35|30762|3266|6|28|47397.28|0.03|0.02|N|O|1995-02-01|1995-12-24|1995-02-28|COLLECT COD|RAIL|ly alongside of |\n"
		+ "36|119767|9768|1|42|75043.92|0.09|0.00|N|O|1996-02-03|1996-01-21|1996-02-23|COLLECT COD|SHIP| careful courts. special |\n"
		+ "37|22630|5133|1|40|62105.20|0.09|0.03|A|F|1995-07-21|1995-08-01|1995-08-15|NONE|REG AIR|luffily regular requests. slyly final acco|\n"
		+ "37|126782|1807|2|39|70542.42|0.05|0.02|A|F|1995-07-02|1995-08-18|1995-07-28|TAKE BACK RETURN|RAIL|the final requests. ca|\n"
		+ "37|12903|5405|3|43|78083.70|0.05|0.08|A|F|1995-07-10|1995-07-06|1995-08-02|DELIVER IN PERSON|TRUCK|iously ste|\n"
		+ "38|175839|874|1|44|84252.52|0.04|0.02|N|O|1996-09-29|1996-11-17|1996-09-30|COLLECT COD|MAIL|s. blithely unusual theodolites am|\n"
		+ "39|2320|9821|1|44|53782.08|0.09|0.06|N|O|1996-11-14|1996-12-15|1996-12-12|COLLECT COD|RAIL|eodolites. careful|\n"
		+ "39|186582|4137|2|26|43383.08|0.08|0.04|N|O|1996-11-04|1996-10-20|1996-11-20|NONE|FOB|ckages across the slyly silent|\n"
		+ "39|67831|5350|3|46|82746.18|0.06|0.08|N|O|1996-09-26|1996-12-19|1996-10-26|DELIVER IN PERSON|AIR|he carefully e|\n"
		+ "39|20590|3093|4|32|48338.88|0.07|0.05|N|O|1996-10-02|1996-12-19|1996-10-14|COLLECT COD|MAIL|heodolites sleep silently pending foxes. ac|\n"
		+ "39|54519|9530|5|43|63360.93|0.01|0.01|N|O|1996-10-17|1996-11-14|1996-10-26|COLLECT COD|MAIL|yly regular i|\n"
		+ "39|94368|6878|6|40|54494.40|0.06|0.05|N|O|1996-12-08|1996-10-22|1997-01-01|COLLECT COD|AIR|quickly ironic fox|\n"
		+ "64|85951|5952|1|21|40675.95|0.05|0.02|R|F|1994-09-30|1994-09-18|1994-10-26|DELIVER IN PERSON|REG AIR|ch slyly final, thin platelets.|\n"
		+ "65|59694|4705|1|26|42995.94|0.03|0.03|A|F|1995-04-20|1995-04-25|1995-05-13|NONE|TRUCK|pending deposits nag even packages. ca|\n"
		+ "65|73815|8830|2|22|39353.82|0.00|0.05|N|O|1995-07-17|1995-06-04|1995-05-19|COLLECT COD|FOB| ideas. special, r|\n"
		+ "65|1388|3889|3|21|27076.98|0.09|0.07|N|O|1995-07-06|1995-05-14|1995-07-31|DELIVER IN PERSON|RAIL|bove the even packages. accounts nag carefu|\n"
		+ "66|115118|7630|1|31|35126.41|0.00|0.08|R|F|1994-02-19|1994-03-11|1994-02-20|TAKE BACK RETURN|RAIL|ut the unusual accounts sleep at the bo|\n"
		+ "66|173489|3490|2|41|64061.68|0.04|0.07|A|F|1994-02-21|1994-03-01|1994-03-18|COLLECT COD|AIR| regular de|\n"
		+ "67|21636|9143|1|4|6230.52|0.09|0.04|N|O|1997-04-17|1997-01-31|1997-04-20|NONE|SHIP| cajole thinly expres|\n"
		+ "67|20193|5198|2|12|13358.28|0.09|0.05|N|O|1997-01-27|1997-02-21|1997-02-22|NONE|REG AIR| even packages cajole|\n"
		+ "67|173600|6118|3|5|8368.00|0.03|0.07|N|O|1997-02-20|1997-02-12|1997-02-21|DELIVER IN PERSON|TRUCK|y unusual packages thrash pinto |\n"
		+ "67|87514|7515|4|44|66066.44|0.08|0.06|N|O|1997-03-18|1997-01-29|1997-04-13|DELIVER IN PERSON|RAIL|se quickly above the even, express reques|\n"
		+ "67|40613|8126|5|23|35733.03|0.05|0.07|N|O|1997-04-19|1997-02-14|1997-05-06|DELIVER IN PERSON|REG AIR|ly regular deposit|\n"
		+ "67|178306|824|6|29|40144.70|0.02|0.05|N|O|1997-01-25|1997-01-27|1997-01-27|DELIVER IN PERSON|FOB|ultipliers |\n";

	private static final String EXPECTED_RESULT = "1-URGENT|2|\n" + "3-MEDIUM|2|\n" + "4-NOT SPECIFIED|4|";

	@Override
	protected void preSubmit() throws Exception {
		ordersPath = createTempFile("orders", ORDERS);
		lineitemsPath = createTempFile("lineitems", LINEITEMS);
		resultPath = getTempDirPath("result");
	}
	
	@Override
	protected Plan getTestJob() {
		TPCHQuery4 tpch4 = new TPCHQuery4();
		return tpch4.getPlan("4", ordersPath, lineitemsPath, resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}
