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
import eu.stratosphere.test.testPrograms.tpch9.TPCHQuery9;
import eu.stratosphere.test.util.RecordAPITestBase;

public class TPCHQuery9ITCase extends RecordAPITestBase {
	
	private String partInputPath;
	private String partSuppInputPath;
	private String ordersInputPath;
	private String lineItemInputPath;
	private String supplierInputPath;
	private String nationInputPath;
	private String resultPath;

	private static final String PART = ""
		+ "1|goldenrod lavender spring chocolate lace|Manufacturer#1|Brand#13|PROMO BURNISHED COPPER|7|JUMBO PKG|901.00|ly. slyly ironi|\n"
		+ "2|maroon sky cream royal snow|Manufacturer#1|Brand#13|LARGE BRUSHED BRASS|1|LG CASE|902.00|lar accounts amo|\n"
		+ "3|brown blue puff midnight black|Manufacturer#4|Brand#42|STANDARD POLISHED BRASS|21|WRAP CASE|903.00|egular deposits hag|\n"
		+ "4|orange goldenrod peach misty seashell|Manufacturer#3|Brand#34|SMALL PLATED BRASS|14|MED DRUM|904.00|p furiously r|\n"
		+ "5|midnight linen almond tomato plum|Manufacturer#3|Brand#32|STANDARD POLISHED TIN|15|SM PKG|905.00| wake carefully |\n"
		+ "6|deep ivory light pink cream|Manufacturer#2|Brand#24|PROMO PLATED STEEL|4|MED BAG|906.00|sual a|\n"
		+ "7|smoke magenta midnight purple blanched|Manufacturer#1|Brand#11|SMALL PLATED COPPER|45|SM BAG|907.00|lyly. ex|\n"
		+ "8|floral moccasin violet yellow sky|Manufacturer#4|Brand#44|PROMO BURNISHED TIN|41|LG DRUM|908.00|eposi|\n"
		+ "9|coral chiffon rose red saddle|Manufacturer#4|Brand#43|SMALL BURNISHED STEEL|12|WRAP CASE|909.00|ironic foxe|\n"
		+ "10|grey orchid mint purple misty|Manufacturer#5|Brand#54|LARGE BURNISHED STEEL|44|LG CAN|910.01|ithely final deposit|\n"
		+ "11|tomato navy linen grey maroon|Manufacturer#2|Brand#25|STANDARD BURNISHED NICKEL|43|WRAP BOX|911.01|ng gr|\n"
		+ "12|yellow salmon wheat blanched purple|Manufacturer#3|Brand#33|MEDIUM ANODIZED STEEL|25|JUMBO CASE|912.01| quickly|\n"
		+ "13|steel black tomato lemon aquamarine|Manufacturer#5|Brand#55|MEDIUM BURNISHED NICKEL|1|JUMBO PACK|913.01|osits.|\n"
		+ "14|sienna gainsboro cornsilk lavender blush|Manufacturer#1|Brand#13|SMALL POLISHED STEEL|28|JUMBO BOX|914.01|kages c|\n"
		+ "15|ivory khaki black plum medium|Manufacturer#1|Brand#15|LARGE ANODIZED BRASS|45|LG CASE|915.01|usual ac|\n"
		+ "16|maroon cornsilk steel slate navy|Manufacturer#3|Brand#32|PROMO PLATED TIN|2|MED PACK|916.01|unts a|\n"
		+ "17|spring grey turquoise cyan magenta|Manufacturer#4|Brand#43|ECONOMY BRUSHED STEEL|16|LG BOX|917.01| regular accounts|\n"
		+ "18|pale lace powder dim ivory|Manufacturer#1|Brand#11|SMALL BURNISHED STEEL|42|JUMBO PACK|918.01|s cajole slyly a|\n"
		+ "19|hot aquamarine green khaki light|Manufacturer#2|Brand#23|SMALL ANODIZED NICKEL|33|WRAP BOX|919.01| pending acc|\n"
		+ "20|sky chiffon burnished spring powder|Manufacturer#1|Brand#12|LARGE POLISHED NICKEL|48|MED BAG|920.02|are across the asympt|\n"
		+ "21|yellow snow spring sandy antique|Manufacturer#3|Brand#33|SMALL BURNISHED TIN|31|MED BAG|921.02|ss packages. pendin|\n"
		+ "22|dark gainsboro medium cream burnished|Manufacturer#4|Brand#43|PROMO POLISHED BRASS|19|LG DRUM|922.02| even p|\n"
		+ "23|pale seashell olive chartreuse tomato|Manufacturer#3|Brand#35|MEDIUM BURNISHED TIN|42|JUMBO JAR|923.02|nic, fina|\n"
		+ "24|violet lemon grey navajo turquoise|Manufacturer#5|Brand#52|MEDIUM PLATED STEEL|20|MED CASE|924.02| final the|\n"
		+ "25|grey chocolate antique dark ghost|Manufacturer#5|Brand#55|STANDARD BRUSHED COPPER|3|JUMBO BAG|925.02|requests wake|\n"
		+ "26|ghost violet maroon khaki saddle|Manufacturer#3|Brand#32|SMALL BRUSHED STEEL|32|SM CASE|926.02| instructions i|\n"
		+ "27|navy dim saddle indian midnight|Manufacturer#1|Brand#14|LARGE ANODIZED TIN|20|MED PKG|927.02|s wake. ir|\n"
		+ "28|orchid burnished metallic chiffon red|Manufacturer#4|Brand#44|SMALL PLATED COPPER|19|JUMBO PKG|928.02|x-ray pending, iron|\n"
		+ "29|aquamarine puff purple drab pale|Manufacturer#3|Brand#33|PROMO PLATED COPPER|7|LG DRUM|929.02| carefully fluffi|\n"
		+ "30|brown chiffon firebrick blanched smoke|Manufacturer#4|Brand#42|PROMO ANODIZED TIN|17|LG BOX|930.03|carefully bus|\n"
		+ "31|cream goldenrod linen almond tomato|Manufacturer#5|Brand#53|STANDARD BRUSHED TIN|10|LG BAG|931.03|uriously s|\n"
		+ "32|blanched purple maroon tan slate|Manufacturer#4|Brand#42|ECONOMY PLATED BRASS|31|LG CASE|932.03|urts. carefully fin|\n"
		+ "33|saddle cream tomato cyan chartreuse|Manufacturer#2|Brand#22|ECONOMY PLATED NICKEL|16|LG PKG|933.03|ly eve|\n"
		+ "34|purple lawn ghost steel azure|Manufacturer#1|Brand#13|LARGE BRUSHED STEEL|8|JUMBO BOX|934.03|riously ironic|\n"
		+ "35|puff white cornsilk green forest|Manufacturer#4|Brand#43|MEDIUM ANODIZED BRASS|14|JUMBO PACK|935.03|e carefully furi|\n"
		+ "36|slate frosted violet sienna dark|Manufacturer#2|Brand#25|SMALL BURNISHED COPPER|3|JUMBO CAN|936.03|olites o|\n"
		+ "37|floral khaki light drab almond|Manufacturer#4|Brand#45|LARGE POLISHED TIN|48|JUMBO BOX|937.03|silent |\n"
		+ "38|snow lavender slate midnight forest|Manufacturer#4|Brand#43|ECONOMY ANODIZED BRASS|11|SM JAR|938.03|structions inte|\n"
		+ "39|navy rosy antique olive burlywood|Manufacturer#5|Brand#53|SMALL POLISHED TIN|43|JUMBO JAR|939.03|se slowly above the fl|\n"
		+ "40|misty lace snow thistle saddle|Manufacturer#2|Brand#25|ECONOMY BURNISHED COPPER|27|SM CASE|940.04|! blithely specia|\n"
		+ "41|moccasin maroon dim cream frosted|Manufacturer#2|Brand#23|ECONOMY ANODIZED TIN|7|WRAP JAR|941.04|uriously. furiously cl|\n"
		+ "42|blanched lace magenta frosted rosy|Manufacturer#5|Brand#52|MEDIUM BURNISHED TIN|45|LG BOX|942.04|the slow|\n"
		+ "43|aquamarine steel indian cornflower chiffon|Manufacturer#4|Brand#44|PROMO POLISHED STEEL|5|WRAP CASE|943.04|e slyly along the ir|\n"
		+ "44|red light midnight wheat chartreuse|Manufacturer#4|Brand#45|MEDIUM PLATED TIN|48|SM PACK|944.04|pinto beans. carefully|\n"
		+ "45|green dodger beige peru navajo|Manufacturer#4|Brand#43|SMALL BRUSHED NICKEL|9|WRAP BAG|945.04|nts bo|\n"
		+ "46|black brown beige rose slate|Manufacturer#1|Brand#11|STANDARD POLISHED TIN|45|WRAP CASE|946.04|the blithely unusual |\n"
		+ "47|green cyan rose misty pale|Manufacturer#4|Brand#45|LARGE BURNISHED BRASS|14|JUMBO PACK|947.04| even plate|\n"
		+ "48|navajo almond royal forest cornflower|Manufacturer#5|Brand#53|STANDARD BRUSHED STEEL|27|JUMBO CASE|948.04|ng to the depo|\n"
		+ "49|cyan powder lime chartreuse goldenrod|Manufacturer#2|Brand#24|SMALL BURNISHED TIN|31|MED DRUM|949.04|ar pack|\n"
		+ "50|peach maroon chiffon lawn red|Manufacturer#3|Brand#33|LARGE ANODIZED TIN|25|WRAP PKG|950.05|kages m|\n"
		+ "51|rosy linen royal drab floral|Manufacturer#4|Brand#45|ECONOMY BURNISHED NICKEL|34|JUMBO PACK|951.05|n foxes|\n"
		+ "52|lemon salmon snow forest blush|Manufacturer#3|Brand#35|STANDARD BURNISHED TIN|25|WRAP CASE|952.05| final deposits. fu|\n"
		+ "53|blue burlywood magenta gainsboro sandy|Manufacturer#2|Brand#23|ECONOMY BURNISHED NICKEL|32|MED BAG|953.05|mptot|\n"
		+ "54|hot sienna cornsilk saddle dark|Manufacturer#2|Brand#21|LARGE BURNISHED COPPER|19|WRAP CASE|954.05|e blithely|\n"
		+ "55|honeydew chocolate magenta steel lavender|Manufacturer#2|Brand#23|ECONOMY BRUSHED COPPER|9|MED BAG|955.05|ly final pac|\n"
		+ "56|chocolate lavender forest tomato aquamarine|Manufacturer#1|Brand#12|MEDIUM PLATED STEEL|20|WRAP DRUM|956.05|ts. blithel|\n";
	
	private static final String PARTSUPP = ""
		+ "1|2|3325|771.64|, even theodolites. regular, final theodolites eat after the carefully pending foxes. furiously regular deposits sleep slyly. carefully bold realms above the ironic dependencies haggle careful|\n"
		+ "1|252|8076|993.49|ven ideas. quickly even packages print. pending multipliers must have to are fluff|\n"
		+ "19|502|3956|337.09|after the fluffily ironic deposits? blithely special dependencies integrate furiously even excuses. blithely silent theodolites could have to haggle pending, express requests; fu|\n"
		+ "1|752|4069|357.84|al, regular dependencies serve carefully after the quickly final pinto beans. furiously even deposits sleep quickly final, silent pinto beans. fluffily reg|\n"
		+ "2|3|8895|378.49|nic accounts. final accounts sleep furiously about the ironic, bold packages. regular, regular accounts|\n"
		+ "2|253|4969|915.27|ptotes. quickly pending dependencies integrate furiously. fluffily ironic ideas impress blithely above the express accounts. furiously even epitaphs need to wak|\n"
		+ "2|503|8539|438.37|blithely bold ideas. furiously stealthy packages sleep fluffily. slyly special deposits snooze furiously carefully regular accounts. regular deposits according to the accounts nag carefully slyl|\n"
		+ "2|753|3025|306.39|olites. deposits wake carefully. even, express requests cajole. carefully regular ex|\n"
		+ "3|4|4651|920.92|ilent foxes affix furiously quickly unusual requests. even packages across the carefully even theodolites nag above the sp|\n"
		+ "3|254|4093|498.13|ending dependencies haggle fluffily. regular deposits boost quickly carefully regular requests. deposits affix furiously around the pinto beans. ironic, unusual platelets across the p|\n"
		+ "3|504|3917|645.40|of the blithely regular theodolites. final theodolites haggle blithely carefully unusual ideas. blithely even f|\n"
		+ "3|754|9942|191.92| unusual, ironic foxes according to the ideas detect furiously alongside of the even, express requests. blithely regular the|\n"
		+ "4|5|1339|113.97| carefully unusual ideas. packages use slyly. blithely final pinto beans cajole along the furiously express requests. regular orbits haggle carefully. care|\n"
		+ "4|255|6377|591.18|ly final courts haggle carefully regular accounts. carefully regular accounts could integrate slyly. slyly express packages about the accounts wake slyly|\n"
		+ "4|505|2694|51.37|g, regular deposits: quick instructions run across the carefully ironic theodolites-- final dependencies haggle into the dependencies. f|\n"
		+ "4|755|2480|444.37|requests sleep quickly regular accounts. theodolites detect. carefully final depths w|\n"
		+ "5|6|3735|255.88|arefully even requests. ironic requests cajole carefully even dolphin|\n"
		+ "5|256|9653|50.52|y stealthy deposits. furiously final pinto beans wake furiou|\n"
		+ "5|506|1329|219.83|iously regular deposits wake deposits. pending pinto beans promise ironic dependencies. even, regular pinto beans integrate|\n"
		+ "5|756|6925|537.98|sits. quickly fluffy packages wake quickly beyond the blithely regular requests. pending requests cajole among the final pinto beans. carefully busy theodolites affix quickly stealthily |\n"
		+ "6|7|8851|130.72|usly final packages. slyly ironic accounts poach across the even, sly requests. carefully pending request|\n"
		+ "6|257|1627|424.25| quick packages. ironic deposits print. furiously silent platelets across the carefully final requests are slyly along the furiously even instructi|\n"
		+ "6|507|3336|642.13|final instructions. courts wake packages. blithely unusual realms along the multipliers nag |\n"
		+ "6|757|6451|175.32| accounts alongside of the slyly even accounts wake carefully final instructions-- ruthless platelets wake carefully ideas. even deposits are quickly final,|\n"
		+ "7|8|7454|763.98|y express tithes haggle furiously even foxes. furiously ironic deposits sleep toward the furiously unusual|\n"
		+ "7|258|2770|149.66|hould have to nag after the blithely final asymptotes. fluffily spe|\n"
		+ "7|508|3377|68.77|usly against the daring asymptotes. slyly regular platelets sleep quickly blithely regular deposits. boldly regular deposits wake blithely ironic accounts|\n"
		+ "7|758|9460|299.58|. furiously final ideas hinder slyly among the ironic, final packages. blithely ironic dependencies cajole pending requests: blithely even packa|\n"
		+ "19|9|6834|249.63|lly ironic accounts solve express, unusual theodolites. special packages use quickly. quickly fin|\n"
		+ "8|259|396|957.34|r accounts. furiously pending dolphins use even, regular platelets. final|\n"
		+ "8|509|9845|220.62|s against the fluffily special packages snooze slyly slyly regular p|\n"
		+ "8|759|8126|916.91|final accounts around the blithely special asymptotes wake carefully beyond the bold dugouts. regular ideas haggle furiously after|\n"
		+ "9|10|7054|84.20|ts boost. evenly regular packages haggle after the quickly careful accounts. |\n"
		+ "9|260|7542|811.84|ate after the final pinto beans. express requests cajole express packages. carefully bold ideas haggle furiously. blithely express accounts eat carefully among the evenly busy accounts. carefully un|\n"
		+ "9|510|9583|381.31|d foxes. final, even braids sleep slyly slyly regular ideas. unusual ideas above|\n"
		+ "9|760|3063|291.84| the blithely ironic instructions. blithely express theodolites nag furiously. carefully bold requests shall have to use slyly pending requests. carefully regular instr|\n"
		+ "19|11|2952|996.12| bold foxes wake quickly even, final asymptotes. blithely even depe|\n"
		+ "10|261|3335|673.27|s theodolites haggle according to the fluffily unusual instructions. silent realms nag carefully ironic theodolites. furiously unusual instructions would detect fu|\n"
		+ "10|511|5691|164.00|r, silent instructions sleep slyly regular pinto beans. furiously unusual gifts use. silently ironic theodolites cajole final deposits! express dugouts are furiously. packages sleep |\n"
		+ "10|761|841|374.02|refully above the ironic packages. quickly regular packages haggle foxes. blithely ironic deposits a|\n"
		+ "11|12|4540|709.87|thely across the blithely unusual requests. slyly regular instructions wake slyly ironic theodolites. requests haggle blithely above the blithely brave p|\n"
		+ "45|27|4729|894.90|ters wake. sometimes bold packages cajole sometimes blithely final instructions. carefully ironic foxes after the furiously unusual foxes cajole carefully acr|\n"
		+ "11|512|3708|818.74|inal accounts nag quickly slyly special frays; bold, final theodolites play slyly after the furiously pending packages. f|\n"
		+ "11|762|3213|471.98|nusual, regular requests use carefully. slyly final packages haggle quickly. slyly express packages impress blithely across the blithely regular ideas. regular depe|\n"
		+ "12|13|3610|659.73|jole bold theodolites. final packages haggle! carefully regular deposits play furiously among the special ideas. quickly ironic packages detect quickly carefully final|\n"
		+ "12|263|7606|332.81|luffily regular courts engage carefully special realms. regular accounts across the blithely special pinto beans use carefully at the silent request|\n"
		+ "12|513|824|337.06|es are unusual deposits. fluffily even deposits across the blithely final theodolites doubt across the unusual accounts. regular, |\n"
		+ "12|763|5454|901.70|s across the carefully regular courts haggle fluffily among the even theodolites. blithely final platelets x-ray even ideas. fluffily express pinto beans sleep slyly. carefully even a|\n"
		+ "13|14|612|169.44|s. furiously even asymptotes use slyly blithely express foxes. pending courts integrate blithely among the ironic requests! blithely pending deposits integrate slyly furiously final packa|\n"
		+ "13|264|7268|862.70|s sleep slyly packages. final theodolites to the express packages haggle quic|\n"
		+ "13|514|864|38.64|s after the slyly pending instructions haggle even, express requests. permanently regular pinto beans are. slyly pending req|\n"
		+ "13|764|9736|327.18|tect after the express instructions. furiously silent ideas sleep blithely special ideas. attainments sleep furiously. carefully bold requests ab|\n"
		+ "14|15|5278|650.07|e quickly among the furiously ironic accounts. special, final sheaves against the|\n"
		+ "14|265|5334|889.50|ss dependencies are furiously silent excuses. blithely ironic pinto beans affix quickly according to the slyly ironic asymptotes. final packag|\n"
		+ "14|515|3676|893.39|sits are according to the fluffily silent asymptotes. final ideas are slyly above the regular instructions. furiousl|\n"
		+ "45|765|4947|310.13| final deposits boost slyly regular packages; carefully pending theodolites |\n";
	
	private static final String ORDERS = ""
		+ "1|3691|O|194029.55|1996-01-02|5-LOW|Clerk#000000951|0|nstructions sleep furiously among |\n"
		+ "2|7801|O|60951.63|1996-12-01|1-URGENT|Clerk#000000880|0| foxes. pending accounts at the pending, silent asymptot|\n"
		+ "3|12332|F|247296.05|1993-10-14|5-LOW|Clerk#000000955|0|sly final accounts boost. carefully regular ideas cajole carefully. depos|\n"
		+ "4|13678|O|53829.87|1995-10-11|5-LOW|Clerk#000000124|0|sits. slyly regular warthogs cajole. regular, regular theodolites acro|\n"
		+ "5|4450|F|139660.54|1994-07-30|5-LOW|Clerk#000000925|0|quickly. bold deposits sleep slyly. packages use slyly|\n"
		+ "6|5563|F|65843.52|1992-02-21|4-NOT SPECIFIED|Clerk#000000058|0|ggle. special, final requests are against the furiously specia|\n"
		+ "7|3914|O|231037.28|1996-01-10|2-HIGH|Clerk#000000470|0|ly special requests |\n"
		+ "32|13006|O|166802.63|1995-07-16|2-HIGH|Clerk#000000616|0|ise blithely bold, regular requests. quickly unusual dep|\n"
		+ "33|6697|F|118518.56|1993-10-27|3-MEDIUM|Clerk#000000409|0|uriously. furiously final request|\n"
		+ "34|6101|O|75662.77|1998-07-21|3-MEDIUM|Clerk#000000223|0|ly final packages. fluffily final deposits wake blithely ideas. spe|\n"
		+ "35|12760|O|192885.43|1995-10-23|4-NOT SPECIFIED|Clerk#000000259|0|zzle. carefully enticing deposits nag furio|\n"
		+ "36|11527|O|72196.43|1995-11-03|1-URGENT|Clerk#000000358|0| quick packages are blithely. slyly silent accounts wake qu|\n"
		+ "37|8612|F|156440.15|1992-06-03|3-MEDIUM|Clerk#000000456|0|kly regular pinto beans. carefully unusual waters cajole never|\n"
		+ "38|12484|O|64695.26|1996-08-21|4-NOT SPECIFIED|Clerk#000000604|0|haggle blithely. furiously express ideas haggle blithely furiously regular re|\n"
		+ "39|8177|O|307811.89|1996-09-20|3-MEDIUM|Clerk#000000659|0|ole express, ironic requests: ir|\n"
		+ "64|3212|F|30616.90|1994-07-16|3-MEDIUM|Clerk#000000661|0|wake fluffily. sometimes ironic pinto beans about the dolphin|\n"
		+ "65|1627|P|99763.79|1995-03-18|1-URGENT|Clerk#000000632|0|ular requests are blithely pending orbits-- even requests against the deposit|\n"
		+ "66|12920|F|100991.26|1994-01-20|5-LOW|Clerk#000000743|0|y pending requests integrate|\n"
		+ "67|5662|O|167270.36|1996-12-19|4-NOT SPECIFIED|Clerk#000000547|0|symptotes haggle slyly around the furiously iron|\n"
		+ "68|2855|O|305815.83|1998-04-18|3-MEDIUM|Clerk#000000440|0| pinto beans sleep carefully. blithely ironic deposits haggle furiously acro|\n"
		+ "69|8449|F|228015.94|1994-06-04|4-NOT SPECIFIED|Clerk#000000330|0| depths atop the slyly thin deposits detect among the furiously silent accou|\n"
		+ "70|6434|F|133507.10|1993-12-18|5-LOW|Clerk#000000322|0| carefully ironic request|\n"
		+ "71|338|O|244449.86|1998-01-24|4-NOT SPECIFIED|Clerk#000000271|0| express deposits along the blithely regul|\n"
		+ "96|10778|F|72504.36|1994-04-17|2-HIGH|Clerk#000000395|0|oost furiously. pinto|\n"
		+ "97|2107|F|128590.11|1993-01-29|3-MEDIUM|Clerk#000000547|0|hang blithely along the regular accounts. furiously even ideas after the|\n"
		+ "98|10448|F|62956.90|1994-09-25|1-URGENT|Clerk#000000448|0|c asymptotes. quickly regular packages should have to nag re|\n"
		+ "99|8891|F|136624.34|1994-03-13|4-NOT SPECIFIED|Clerk#000000973|0|e carefully ironic packages. pending|\n"
		+ "100|14701|O|204408.59|1998-02-28|4-NOT SPECIFIED|Clerk#000000577|0|heodolites detect slyly alongside of the ent|\n"
		+ "101|2800|O|142434.13|1996-03-17|3-MEDIUM|Clerk#000000419|0|ding accounts above the slyly final asymptote|\n"
		+ "102|73|O|172239.95|1997-05-09|2-HIGH|Clerk#000000596|0| slyly according to the asymptotes. carefully final packages integrate furious|\n"
		+ "103|2911|O|147675.81|1996-06-20|4-NOT SPECIFIED|Clerk#000000090|0|ges. carefully unusual instructions haggle quickly regular f|\n"
		+ "128|7396|F|57495.50|1992-06-15|1-URGENT|Clerk#000000385|0|ns integrate fluffily. ironic asymptotes after the regular excuses nag around |\n"
		+ "129|7114|F|273469.52|1992-11-19|5-LOW|Clerk#000000859|0|ing tithes. carefully pending deposits boost about the silently express |\n"
		+ "130|3697|F|169065.69|1992-05-08|2-HIGH|Clerk#000000036|0|le slyly unusual, regular packages? express deposits det|\n"
		+ "131|9275|F|143898.91|1994-06-08|3-MEDIUM|Clerk#000000625|0|after the fluffily special foxes integrate s|\n"
		+ "132|2641|F|180897.80|1993-06-11|3-MEDIUM|Clerk#000000488|0|sits are daringly accounts. carefully regular foxes sleep slyly about the|\n"
		+ "133|4400|O|120535.39|1997-11-29|1-URGENT|Clerk#000000738|0|usly final asymptotes |\n"
		+ "134|620|F|203218.79|1992-05-01|4-NOT SPECIFIED|Clerk#000000711|0|lar theodolites boos|\n"
		+ "135|6049|O|280793.15|1995-10-21|4-NOT SPECIFIED|Clerk#000000804|0|l platelets use according t|\n"
		+ "160|8251|O|114252.21|1996-12-19|4-NOT SPECIFIED|Clerk#000000342|0|thely special sauternes wake slyly of t|\n"
		+ "161|1663|F|22632.04|1994-08-31|2-HIGH|Clerk#000000322|0|carefully! special instructions sin|\n"
		+ "162|1412|O|3658.13|1995-05-08|3-MEDIUM|Clerk#000000378|0|nts hinder fluffily ironic instructions. express, express excuses |\n"
		+ "163|8776|O|172394.12|1997-09-05|3-MEDIUM|Clerk#000000379|0|y final packages. final foxes since the quickly even|\n"
		+ "164|79|F|315228.81|1992-10-21|5-LOW|Clerk#000000209|0|cajole ironic courts. slyly final ideas are slyly. blithely final Tiresias sub|\n"
		+ "165|2725|F|204566.92|1993-01-30|4-NOT SPECIFIED|Clerk#000000292|0|across the blithely regular accounts. bold|\n"
		+ "166|10783|O|152280.99|1995-09-12|2-HIGH|Clerk#000000440|0|lets. ironic, bold asymptotes kindle|\n"
		+ "167|11941|F|53697.73|1993-01-04|4-NOT SPECIFIED|Clerk#000000731|0|s nag furiously bold excuses. fluffily iron|\n"
		+ "192|8257|O|151014.95|1997-11-25|5-LOW|Clerk#000000483|0|y unusual platelets among the final instructions integrate rut|\n"
		+ "193|7907|F|60599.97|1993-08-08|1-URGENT|Clerk#000000025|0|the furiously final pin|\n"
		+ "194|6173|F|169409.25|1992-04-05|3-MEDIUM|Clerk#000000352|0|egular requests haggle slyly regular, regular pinto beans. asymptote|\n"
		+ "195|13543|F|169343.52|1993-12-28|3-MEDIUM|Clerk#000000216|0|old forges are furiously sheaves. slyly fi|\n"
		+ "196|6484|F|55238.37|1993-03-17|2-HIGH|Clerk#000000988|0|beans boost at the foxes. silent foxes|\n"
		+ "197|3253|P|162567.01|1995-04-07|2-HIGH|Clerk#000000969|0|solve quickly about the even braids. carefully express deposits affix care|\n"
		+ "198|11023|O|167234.32|1998-01-02|4-NOT SPECIFIED|Clerk#000000331|0|its. carefully ironic requests sleep. furiously express fox|\n"
		+ "199|5297|O|93001.13|1996-03-07|2-HIGH|Clerk#000000489|0|g theodolites. special packag|\n"
		+ "224|248|F|232428.88|1994-06-18|4-NOT SPECIFIED|Clerk#000000642|0|r the quickly thin courts. carefully|\n";
	
	private static final String LINEITEM = ""
		+ "1|19|785|1|17|24386.67|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|\n"
		+ "1|6731|732|2|36|58958.28|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |\n"
		+ "1|45|27|3|8|10210.96|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep|\n"
		+ "1|214|465|4|28|31197.88|0.09|0.06|N|O|1996-04-21|1996-03-30|1996-05-16|NONE|AIR|lites. fluffily even de|\n"
		+ "1|2403|160|5|24|31329.60|0.10|0.04|N|O|1996-03-30|1996-03-14|1996-04-01|NONE|FOB| pending foxes. slyly re|\n"
		+ "1|1564|67|6|32|46897.92|0.07|0.02|N|O|1996-01-30|1996-02-07|1996-02-03|DELIVER IN PERSON|MAIL|arefully slyly ex|\n"
		+ "2|10617|138|1|38|58049.18|0.00|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|ven requests. deposits breach a|\n"
		+ "3|19|9|1|45|59869.35|0.06|0.00|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco|\n"
		+ "3|19|11|2|49|88489.10|0.10|0.00|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve|\n"
		+ "3|12845|370|3|27|47461.68|0.06|0.07|A|F|1994-01-16|1993-11-22|1994-01-23|DELIVER IN PERSON|SHIP|nal foxes wake. |\n"
		+ "3|45|502|4|2|3681.86|0.01|0.06|A|F|1993-12-04|1994-01-07|1994-01-01|NONE|TRUCK|y. fluffily pending d|\n"
		+ "3|45|765|5|28|34392.68|0.04|0.00|R|F|1993-12-14|1994-01-10|1994-01-01|TAKE BACK RETURN|FOB|ages nag slyly pending|\n"
		+ "3|6215|984|6|26|29151.46|0.10|0.02|A|F|1993-10-29|1993-12-18|1993-11-04|TAKE BACK RETURN|RAIL|ges sleep after the caref|\n"
		+ "4|8804|579|1|30|51384.00|0.03|0.08|N|O|1996-01-10|1995-12-14|1996-01-18|DELIVER IN PERSON|REG AIR|- quickly regular packages sleep. idly|\n"
		+ "5|10857|858|1|15|26517.75|0.02|0.04|R|F|1994-10-31|1994-08-31|1994-11-20|NONE|AIR|ts wake furiously |\n"
		+ "5|12393|394|2|26|33940.14|0.07|0.08|R|F|1994-10-16|1994-09-25|1994-10-19|NONE|FOB|sts use slyly quickly special instruc|\n"
		+ "5|19|8|3|50|82887.50|0.08|0.03|A|F|1994-08-08|1994-10-13|1994-08-26|DELIVER IN PERSON|AIR|eodolites. fluffily unusual|\n"
		+ "6|45|27|1|37|69484.52|0.08|0.03|A|F|1992-04-27|1992-05-15|1992-05-02|TAKE BACK RETURN|TRUCK|p furiously special foxes|\n"
		+ "7|45|27|1|12|13490.40|0.07|0.03|N|O|1996-05-07|1996-03-13|1996-06-03|TAKE BACK RETURN|FOB|ss pinto beans wake against th|\n"
		+ "7|19|9|2|9|12955.68|0.08|0.08|N|O|1996-02-01|1996-03-02|1996-02-19|TAKE BACK RETURN|SHIP|es. instructions|\n"
		+ "7|9478|997|3|46|63823.62|0.10|0.07|N|O|1996-01-15|1996-03-27|1996-02-03|COLLECT COD|MAIL| unusual reques|\n"
		+ "7|16308|309|4|28|34280.40|0.03|0.04|N|O|1996-03-21|1996-04-08|1996-04-20|NONE|FOB|. slyly special requests haggl|\n"
		+ "7|19|11|5|38|41997.22|0.08|0.01|N|O|1996-02-11|1996-02-24|1996-02-18|DELIVER IN PERSON|TRUCK|ns haggle carefully ironic deposits. bl|\n"
		+ "7|7926|184|6|35|64187.20|0.06|0.03|N|O|1996-01-16|1996-02-23|1996-01-22|TAKE BACK RETURN|FOB|jole. excuses wake carefully alongside of |\n"
		+ "7|45|765|7|5|8198.60|0.04|0.02|N|O|1996-02-10|1996-03-26|1996-02-13|NONE|FOB|ithely regula|\n"
		+ "32|45|765|1|28|33019.56|0.05|0.08|N|O|1995-10-23|1995-08-27|1995-10-26|TAKE BACK RETURN|TRUCK|sleep quickly. req|\n"
		+ "32|19793|63|2|32|54809.28|0.02|0.00|N|O|1995-08-14|1995-10-07|1995-08-27|COLLECT COD|AIR|lithely regular deposits. fluffily |\n"
		+ "32|19|502|3|2|2642.82|0.09|0.02|N|O|1995-08-07|1995-10-07|1995-08-23|DELIVER IN PERSON|AIR| express accounts wake according to the|\n"
		+ "32|275|776|4|4|4701.08|0.09|0.03|N|O|1995-08-04|1995-10-01|1995-09-03|NONE|REG AIR|e slyly final pac|\n"
		+ "32|19|11|5|44|65585.52|0.05|0.06|N|O|1995-08-28|1995-08-20|1995-09-14|DELIVER IN PERSON|AIR|symptotes nag according to the ironic depo|\n"
		+ "32|1162|414|6|6|6378.96|0.04|0.03|N|O|1995-07-21|1995-09-23|1995-07-25|COLLECT COD|RAIL| gifts cajole carefully.|\n"
		+ "33|6134|903|1|31|32244.03|0.09|0.04|A|F|1993-10-29|1993-12-19|1993-11-08|COLLECT COD|TRUCK|ng to the furiously ironic package|\n"
		+ "33|6052|565|2|32|30657.60|0.02|0.05|A|F|1993-12-09|1994-01-04|1993-12-28|COLLECT COD|MAIL|gular theodolites|\n"
		+ "33|13747|11|3|5|8303.70|0.05|0.03|A|F|1993-12-09|1993-12-25|1993-12-23|TAKE BACK RETURN|AIR|. stealthily bold exc|\n"
		+ "33|19|9|4|41|53110.99|0.09|0.00|R|F|1993-11-09|1994-01-24|1993-11-11|TAKE BACK RETURN|MAIL|unusual packages doubt caref|\n"
		+ "34|8837|92|1|13|22695.79|0.00|0.07|N|O|1998-10-23|1998-09-14|1998-11-06|NONE|REG AIR|nic accounts. deposits are alon|\n"
		+ "34|45|502|2|22|40720.68|0.08|0.06|N|O|1998-10-09|1998-10-16|1998-10-12|NONE|FOB|thely slyly p|\n"
		+ "34|16955|488|3|6|11231.70|0.02|0.06|N|O|1998-10-30|1998-09-20|1998-11-05|NONE|FOB|ar foxes sleep |\n"
		+ "35|45|296|1|24|22680.96|0.02|0.00|N|O|1996-02-21|1996-01-03|1996-03-18|TAKE BACK RETURN|FOB|, regular tithe|\n"
		+ "35|19|765|2|34|37746.46|0.06|0.08|N|O|1996-01-22|1996-01-06|1996-01-27|DELIVER IN PERSON|RAIL|s are carefully against the f|\n"
		+ "35|12090|877|3|7|7014.63|0.06|0.04|N|O|1996-01-19|1995-12-22|1996-01-29|NONE|MAIL| the carefully regular |\n"
		+ "35|8518|777|4|25|35662.75|0.06|0.05|N|O|1995-11-26|1995-12-25|1995-12-21|DELIVER IN PERSON|SHIP| quickly unti|\n"
		+ "35|45|765|5|34|64735.66|0.08|0.06|N|O|1995-11-08|1996-01-15|1995-11-26|COLLECT COD|MAIL|. silent, unusual deposits boost|\n"
		+ "35|3077|331|6|28|27441.96|0.03|0.02|N|O|1996-02-01|1995-12-24|1996-02-28|COLLECT COD|RAIL|ly alongside of |\n"
		+ "36|11977|978|1|42|79336.74|0.09|0.00|N|O|1996-02-03|1996-01-21|1996-02-23|COLLECT COD|SHIP| careful courts. special |\n"
		+ "37|2263|516|1|40|46610.40|0.09|0.03|A|F|1992-07-21|1992-08-01|1992-08-15|NONE|REG AIR|luffily regular requests. slyly final acco|\n"
		+ "37|12679|204|2|39|62075.13|0.05|0.02|A|F|1992-07-02|1992-08-18|1992-07-28|TAKE BACK RETURN|RAIL|the final requests. ca|\n"
		+ "37|19|9|3|43|51268.47|0.05|0.08|A|F|1992-07-10|1992-07-06|1992-08-02|DELIVER IN PERSON|TRUCK|iously ste|\n"
		+ "38|17584|119|1|44|66069.52|0.04|0.02|N|O|1996-09-29|1996-11-17|1996-09-30|COLLECT COD|MAIL|s. blithely unusual theodolites am|\n"
		+ "39|232|983|1|44|49818.12|0.09|0.06|N|O|1996-11-14|1996-12-15|1996-12-12|COLLECT COD|RAIL|eodolites. careful|\n"
		+ "39|18659|464|2|26|41018.90|0.08|0.04|N|O|1996-11-04|1996-10-20|1996-11-20|NONE|FOB|ckages across the slyly silent|\n"
		+ "39|45|27|3|46|77775.88|0.06|0.08|N|O|1996-09-26|1996-12-19|1996-10-26|DELIVER IN PERSON|AIR|he carefully e|\n"
		+ "39|2059|312|4|32|30753.60|0.07|0.05|N|O|1996-10-02|1996-12-19|1996-10-14|COLLECT COD|MAIL|heodolites sleep silently pending foxes. ac|\n"
		+ "39|5452|963|5|43|58370.35|0.01|0.01|N|O|1996-10-17|1996-11-14|1996-10-26|COLLECT COD|MAIL|yly regular i|\n"
		+ "39|9437|697|6|40|53857.20|0.06|0.05|N|O|1996-12-08|1996-10-22|1997-01-01|COLLECT COD|AIR|quickly ironic fox|\n";
	
	private static final String SUPPLIER = ""
		+ "1|Supplier#000000001| N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ|17|27-918-335-1736|5755.94|each slyly above the careful|\n"
		+ "2|Supplier#000000002|89eJ5ksX3ImxJQBvxObC,|5|15-679-861-2259|4032.68| slyly bold instructions. idle dependen|\n"
		+ "3|Supplier#000000003|q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3|1|11-383-516-1199|4192.40|blithely silent requests after the express dependencies are sl|\n"
		+ "4|Supplier#000000004|Bk7ah4CK8SYQTepEmvMkkgMwg|15|25-843-787-7479|4641.08|riously even requests above the exp|\n"
		+ "5|Supplier#000000005|Gcdm2rJRzl5qlTVzc|11|21-151-690-3663|-283.84|. slyly regular pinto bea|\n"
		+ "6|Supplier#000000006|tQxuVm7s7CnK|14|24-696-997-4969|1365.79|final accounts. regular dolphins use against the furiously ironic decoys. |\n"
		+ "7|Supplier#000000007|s,4TicNGB4uO6PaSqNBUq|23|33-990-965-2201|6820.35|s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit|\n"
		+ "8|Supplier#000000008|9Sq4bBH2FQEmaFOocY45sRTxo6yuoG|17|27-498-742-3860|7627.85|al pinto beans. asymptotes haggl|\n"
		+ "9|Supplier#000000009|1KhUgZegwM3ua7dsYmekYBsK|10|20-403-398-8662|5302.37|s. unusual, even requests along the furiously regular pac|\n"
		+ "10|Supplier#000000010|Saygah3gYWMp72i PY|24|34-852-489-8585|3891.91|ing waters. regular requests ar|\n"
		+ "11|Supplier#000000011|JfwTs,LZrV, M,9C|18|28-613-996-1505|3393.08|y ironic packages. slyly ironic accounts affix furiously; ironically unusual excuses across the flu|\n"
		+ "12|Supplier#000000012|aLIW  q0HYd|8|18-179-925-7181|1432.69|al packages nag alongside of the bold instructions. express, daring accounts|\n"
		+ "13|Supplier#000000013|HK71HQyWoqRWOX8GI FpgAifW,2PoH|3|13-727-620-7813|9107.22|requests engage regularly instructions. furiously special requests ar|\n"
		+ "14|Supplier#000000014|EXsnO5pTNj4iZRm|15|25-656-247-5058|9189.82|l accounts boost. fluffily bold warhorses wake|\n"
		+ "15|Supplier#000000015|olXVbNBfVzRqgokr1T,Ie|8|18-453-357-6394|308.56| across the furiously regular platelets wake even deposits. quickly express she|\n"
		+ "16|Supplier#000000016|YjP5C55zHDXL7LalK27zfQnwejdpin4AMpvh|22|32-822-502-4215|2972.26|ously express ideas haggle quickly dugouts? fu|\n"
		+ "17|Supplier#000000017|c2d,ESHRSkK3WYnxpgw6aOqN0q|19|29-601-884-9219|1687.81|eep against the furiously bold ideas. fluffily bold packa|\n"
		+ "18|Supplier#000000018|PGGVE5PWAMwKDZw |16|26-729-551-1115|7040.82|accounts snooze slyly furiously bold |\n"
		+ "19|Supplier#000000019|edZT3es,nBFD8lBXTGeTl|24|34-278-310-2731|6150.38|refully final foxes across the dogged theodolites sleep slyly abou|\n"
		+ "20|Supplier#000000020|iybAE,RmTymrZVYaFZva2SH,j|3|13-715-945-6730|530.82|n, ironic ideas would nag blithely about the slyly regular accounts. silent, expr|\n"
		+ "21|Supplier#000000021|81CavellcrJ0PQ3CPBID0Z0JwyJm0ka5igEs|2|12-253-590-5816|9365.80|d. instructions integrate sometimes slyly pending instructions. accounts nag among the |\n"
		+ "22|Supplier#000000022|okiiQFk 8lm6EVX6Q0,bEcO|4|14-144-830-2814|-966.20| ironically among the deposits. closely expre|\n"
		+ "23|Supplier#000000023|ssetugTcXc096qlD7 2TL5crEEeS3zk|9|19-559-422-5776|5926.41|ges could have to are ironic deposits. regular, even request|\n"
		+ "24|Supplier#000000024|C4nPvLrVmKPPabFCj|0|10-620-939-2254|9170.71|usly pending deposits. slyly final accounts run |\n"
		+ "25|Supplier#000000025|RCQKONXMFnrodzz6w7fObFVV6CUm2q|22|32-431-945-3541|9198.31|ely regular deposits. carefully regular sauternes engage furiously above the regular accounts. idly |\n"
		+ "26|Supplier#000000026|iV,MHzAx6Z939uzFNkq09M0a1 MBfH7|21|31-758-894-4436|21.18| ideas poach carefully after the blithely bold asymptotes. furiously pending theodoli|\n"
		+ "27|Supplier#000000027|lC4CjKwNHUr6L4xIpzOBK4NlHkFTg|18|28-708-999-2028|1887.62|s according to the quickly regular hockey playe|\n"
		+ "28|Supplier#000000028|GBhvoRh,7YIN V|0|10-538-384-8460|-891.99|ld requests across the pinto beans are carefully against the quickly final courts. accounts sleep |\n"
		+ "29|Supplier#000000029|658tEqXLPvRd6xpFdqC2|1|11-555-705-5922|-811.62|y express ideas play furiously. even accounts sleep fluffily across the accounts. careful|\n"
		+ "30|Supplier#000000030|84NmC1rmQfO0fj3zkobLT|16|26-940-594-4852|8080.14|ias. carefully silent accounts cajole blithely. pending, special accounts cajole quickly above the f|\n"
		+ "31|Supplier#000000031|fRJimA7zchyApqRLHcQeocVpP|16|26-515-530-4159|5916.91|into beans wake after the special packages. slyly fluffy requests cajole furio|\n"
		+ "32|Supplier#000000032|yvoD3TtZSx1skQNCK8agk5bZlZLug|23|33-484-637-7873|3556.47|usly even depths. quickly ironic theodolites s|\n"
		+ "33|Supplier#000000033|gfeKpYw3400L0SDywXA6Ya1Qmq1w6YB9f3R|7|17-138-897-9374|8564.12|n sauternes along the regular asymptotes are regularly along the |\n"
		+ "34|Supplier#000000034|mYRe3KvA2O4lL4HhxDKkkrPUDPMKRCSp,Xpa|10|20-519-982-2343|237.31|eposits. slyly final deposits toward the slyly regular dependencies sleep among the excu|\n"
		+ "35|Supplier#000000035|QymmGXxjVVQ5OuABCXVVsu,4eF gU0Qc6|21|31-720-790-5245|4381.41| ironic deposits! final, bold platelets haggle quickly quickly pendin|\n"
		+ "36|Supplier#000000036|mzSpBBJvbjdx3UKTW3bLFewRD78D91lAC879|13|23-273-493-3679|2371.51|ular theodolites must haggle regular, bold accounts. slyly final pinto beans bo|\n"
		+ "37|Supplier#000000037|cqjyB5h1nV|0|10-470-144-1330|3017.47|iously final instructions. quickly special accounts hang fluffily above the accounts. deposits|\n"
		+ "38|Supplier#000000038|xEcx45vD0FXHT7c9mvWFY|4|14-361-296-6426|2512.41|ins. fluffily special accounts haggle slyly af|\n"
		+ "39|Supplier#000000039|ZM, nSYpEPWr1yAFHaC91qjFcijjeU5eH|8|18-851-856-5633|6115.65|le slyly requests. special packages shall are blithely. slyly unusual packages sleep |\n"
		+ "40|Supplier#000000040|zyIeWzbbpkTV37vm1nmSGBxSgd2Kp|22|32-231-247-6991|-290.06| final patterns. accounts haggle idly pas|\n";

	private static final String NATION = ""
		+ "0|ALGERIA|0| haggle. carefully final deposits detect slyly agai|\n"
		+ "1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|\n"
		+ "2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special |\n"
		+ "3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold|\n"
		+ "4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d|\n"
		+ "5|ETHIOPIA|0|ven packages wake quickly. regu|\n"
		+ "6|FRANCE|3|refully final requests. regular, ironi|\n"
		+ "7|GERMANY|3|l platelets. regular accounts x-ray: unusual, regular acco|\n"
		+ "8|INDIA|2|ss excuses cajole slyly across the packages. deposits print aroun|\n"
		+ "9|INDONESIA|2| slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull|\n"
		+ "10|IRAN|4|efully alongside of the slyly final dependencies. |\n"
		+ "11|IRAQ|4|nic deposits boost atop the quickly final requests? quickly regula|\n"
		+ "12|JAPAN|2|ously. final, express gifts cajole a|\n"
		+ "13|JORDAN|4|ic deposits are blithely about the carefully regular pa|\n"
		+ "14|KENYA|0| pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t|\n"
		+ "15|MOROCCO|0|rns. blithely bold courts among the closely regular packages use furiously bold platelets?|\n"
		+ "16|MOZAMBIQUE|0|s. ironic, unusual asymptotes wake blithely r|\n"
		+ "17|PERU|1|platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun|\n"
		+ "18|CHINA|2|c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos|\n"
		+ "19|ROMANIA|3|ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account|\n"
		+ "20|SAUDI ARABIA|4|ts. silent requests haggle. closely express packages sleep across the blithely|\n"
		+ "21|VIETNAM|2|hely enticingly express accounts. even, final |\n"
		+ "22|RUSSIA|3| requests against the platelets use never according to the quickly regular pint|\n"
		+ "23|UNITED KINGDOM|3|eans boost carefully special requests. accounts are. carefull|\n"
		+ "24|UNITED STATES|1|y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be|\n";
	
	private static final String EXPECTED_RESULT = ""
		+ "CHINA|1992|30814.46\n"
		+ "CHINA|1993|30830.309\n"
		+ "CHINA|1995|18476.965\n"
		+ "CHINA|1996|36566.742\n"
		+ "IRAN|1992|37970.953\n"
		+ "IRAN|1993|83140.0\n"
		+ "IRAN|1996|9672.556\n";
		

	@Override
	protected void preSubmit() throws Exception {
		partInputPath = createTempFile("part", PART);
		partSuppInputPath = createTempFile("partSupp", PARTSUPP);
		ordersInputPath = createTempFile("orders", ORDERS);
		lineItemInputPath = createTempFile("lineItem", LINEITEM);
		supplierInputPath = createTempFile("supplier", SUPPLIER);
		nationInputPath = createTempFile("nation", NATION);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected Plan getTestJob() {
		TPCHQuery9 tpch9 = new TPCHQuery9();
		return tpch9.getPlan(
				"4",
				partInputPath,
				partSuppInputPath,
				ordersInputPath,
				lineItemInputPath,
				supplierInputPath,
				nationInputPath,
				resultPath);
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}