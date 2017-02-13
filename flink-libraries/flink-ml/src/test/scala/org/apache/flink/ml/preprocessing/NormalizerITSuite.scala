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

package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{DenseVector, Norm, Vector}
import org.apache.flink.ml.util.FlinkTestBase
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.Breeze._
import org.scalatest.{FlatSpec, Matchers}


class NormalizerSuite extends FlatSpec
  with Matchers
  with FlinkTestBase {

  val epsilon = 0.000001

  behavior of "Flink's Normalizer"

  it should "scale the vectors' values to unit measure based on L^1 norm" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import NormalizerData._

    val dataSet = env.fromCollection(data)
    val normalizer = Normalizer().setNorm(Norm.L1)
    val scaledVectors = normalizer.transform(dataSet).collect()

    scaledVectors.length should equal(data.length)

    for (vector <- scaledVectors) {
      val expectedMeasure = Norm.norm(vector, Norm.L1)
      // check for finite norms only on scaled vectors
      if (!expectedMeasure.isInfinity && !expectedMeasure.isNaN && expectedMeasure > 0.0) {
        ((1.0 - expectedMeasure) < epsilon) shouldEqual true
        val test = vector.asBreeze.forall(fv => {
          fv <= 1.0
        })
        test shouldEqual true
      }
    }
  }

  it should "scale the labeled vectors' values to unit measure based on L^1 norm" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import NormalizerData._

    val dataSet = env.fromCollection(labeledData)
    val normalizer = Normalizer().setNorm(Norm.L1)
    val scaledLabeledVectors = normalizer.transform(dataSet).collect()

    for (labeledVector <- scaledLabeledVectors) {
      val expectedMeasure = Norm.norm(labeledVector.vector, Norm.L1)
      // check for finite norms only on scaled vectors
      if (!expectedMeasure.isInfinity && !expectedMeasure.isNaN && expectedMeasure > 0.0) {
        ((1.0 - expectedMeasure) < epsilon) shouldEqual true
          val test = labeledVector.vector.asBreeze.forall(fv => {
            fv <= 1.0
          })
          test shouldEqual true
        }
    }
  }

  it should "scale the vectors' values to unit measure based on L^2 norm" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import NormalizerData._

    val dataSet = env.fromCollection(data)
    val normalizer = Normalizer().setNorm(Norm.L2)
    val scaledVectors = normalizer.transform(dataSet).collect()

    scaledVectors.length should equal(data.length)

    for (vector <- scaledVectors) {
      val expectedMeasure = Norm.norm(vector, Norm.L2)
      // check for finite norms only on scaled vectors
      if (!expectedMeasure.isInfinity && !expectedMeasure.isNaN && expectedMeasure > 0.0) {
        ((1.0 - expectedMeasure) < epsilon) shouldEqual true
        val test = vector.asBreeze.forall(fv => {
          fv <= 1.0
        })
        test shouldEqual true
      }
    }
  }

  it should "scale the labeled vectors' values to unit measure based on L^2 norm" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import NormalizerData._

    val dataSet = env.fromCollection(labeledData)
    val normalizer = Normalizer().setNorm(Norm.L2)
    val scaledLabeledVectors = normalizer.transform(dataSet).collect()

    for (labeledVector <- scaledLabeledVectors) {
      val expectedMeasure = Norm.norm(labeledVector.vector, Norm.L2)
      // check for finite norms only on scaled vectors
      if (!expectedMeasure.isInfinity && !expectedMeasure.isNaN && expectedMeasure> 0.0) {
        ((1.0 - expectedMeasure) < epsilon) shouldEqual true
        val test = labeledVector.vector.asBreeze.forall(fv => {
          fv <= 1.0
        })
        test shouldEqual true
      }
    }
  }

  it should "handle bad data correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import NormalizerData._

    val normalizer = Normalizer().setNorm(Norm.L1)

    val dataSets = List(badDataCase1, badDataCase2)
      .map(dataSet => env.fromCollection(dataSet))

    val labeledDataSets = List(badDataLabeledCase1, badDataLabeledCase2)
      .map(dataSet => env.fromCollection(dataSet))

    dataSets.foreach { badDataSet =>
      intercept[JobExecutionException] {
        normalizer.transform(badDataSet).collect()
      }
    }

    labeledDataSets.foreach { badDataSet =>
      intercept[JobExecutionException] {
        normalizer.transform(badDataSet).collect()
      }
    }
  }

  it should "handle zero arrays by returning the array itself" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import NormalizerData._

    val normalizer = Normalizer().setNorm(Norm.L1)

    val zeroDataSet = env.fromCollection(zeroData)
    val zeroLabeledDataSet = env.fromCollection(zeroLabeledData)

      normalizer
        .transform(zeroDataSet)
        .collect()
        .head
        .equals(zeroData.head) shouldEqual true

      normalizer
        .transform(zeroLabeledDataSet)
        .collect()
        .head
        .equals(zeroLabeledData.head) shouldEqual true
  }
}

object NormalizerData {
  val data: Seq[Vector] = List(
    DenseVector(Array(3209.15290064,9241.62379838,4694.41856019,9600.59050017)),
    DenseVector(Array(5650.40691893,5018.16229224,3459.93340844,4809.49171244)),
    DenseVector(Array(4230.51845135,3501.59696994,6096.42378941,9188.25847617)),
    DenseVector(Array(2295.53042919,2132.73954312,5533.14437642,1695.41126482)),
    DenseVector(Array(6328.14337606,6023.87035941,5731.24560301,9473.40314977)),
    DenseVector(Array(3139.31104292,4076.73896096,8669.34941141,9385.10836923)),
    DenseVector(Array(917.213204122,5002.11776958,9089.23842563,4969.56563664)),
    DenseVector(Array(3566.61040761,2221.27506337,4010.34816909,8491.33609981)),
    DenseVector(Array(8407.74787651,3392.45330616,2201.36089583,6102.22225705)),
    DenseVector(Array(9662.69840757,250.242785141,155.618732705,7087.07892356)),
    DenseVector(Array(1359.82732021,3506.15347804,8857.73739536,6641.11613179)),
    DenseVector(Array(6581.35090653,7712.62588284,9375.38927725,8177.77707667)),
    DenseVector(Array(6532.32448802,5369.99407886,9651.90828686,95.2254336304)),
    DenseVector(Array(3780.64291305,4771.64443103,2450.17360226,2916.90471518)),
    DenseVector(Array(1960.6862273,2777.1398196,1291.18317963,5382.74703499)),
    DenseVector(Array(5502.54030929,5010.10003643,9960.6265882,7677.71473027)),
    DenseVector(Array(9557.82783598,1635.04517957,5726.46287791,7978.39972038)),
    DenseVector(Array(520.585930199,4230.01236394,7653.3742615,2216.23785338)),
    DenseVector(Array(5174.52571531,6377.1888208,2853.52247774,9145.86087514)),
    DenseVector(Array(1524.21264512,9911.79755268,2509.9536154,4904.38170918)),
    DenseVector(Array(8805.04020456,8079.56209277,8645.742813,3943.66480581)),
    DenseVector(Array(1760.37664106,268.731378488,8927.31694389,9019.72401437)),
    DenseVector(Array(2736.60140737,6028.56969282,6557.9716998,7035.04848801)),
    DenseVector(Array(769.701435612,4871.41484869,2980.42654419,4287.23521538)),
    DenseVector(Array(5262.49232208,9423.82405041,2921.22719345,7815.92134421)),
    DenseVector(Array(935.182129395,3354.13913759,8586.74694351,453.615125771)),
    DenseVector(Array(4164.26691058,8504.02286445,3617.46427402,6050.48310178)),
    DenseVector(Array(8635.11524097,3903.7097401,9688.84566647,2991.59599108)),
    DenseVector(Array(2067.24865755,969.744148553,443.335308226,5552.73797966)),
    DenseVector(Array(5664.53282395,2094.91396363,7093.87830242,5202.23760589)),
    DenseVector(Array(8153.69359583,2398.3919841,4113.98597989,644.427943116)),
    DenseVector(Array(2414.64080857,2583.28164913,7008.40428463,9485.45916776)),
    DenseVector(Array(6632.06415287,8872.60829179,2081.48271649,4207.06700842)),
    DenseVector(Array(3228.18970603,8108.29628313,3830.93183333,2896.91442895)),
    DenseVector(Array(2559.00692051,1806.13205768,337.616812011,6368.01366905)),
    DenseVector(Array(2725.05597429,8015.62070715,4164.4998556,6.54585772873)),
    DenseVector(Array(2441.40349903,2229.97959161,9838.34551407,2387.57662387)),
    DenseVector(Array(4270.8349503,4796.65558363,5967.73402402,7033.86651763)),
    DenseVector(Array(9166.86354147,6467.2601523,1907.01977207,3027.87277783)),
    DenseVector(Array(2224.28919511,5105.09541021,5991.04436647,5873.77890456)),
    DenseVector(Array(3042.12610731,7737.70557794,5368.93852956,4796.85964654)),
    DenseVector(Array(5572.56958277,3647.9470993,7901.3925559,544.857412564)),
    DenseVector(Array(6389.4754591,4124.86615413,9200.02388833,3746.05542784)),
    DenseVector(Array(4818.28251226,6459.49410677,2540.33672203,8246.62569346)),
    DenseVector(Array(4753.19429628,8491.43690912,4936.18102633,4427.38260856)),
    DenseVector(Array(46.5803350637,4045.54953628,7506.71865288,7865.26914029)),
    DenseVector(Array(4643.48679241,4075.32354673,4978.32638772,6553.43734111)),
    DenseVector(Array(6588.78001223,6955.19740018,6823.85346049,4131.14949792)),
    DenseVector(Array(8483.83856093,132.589634347,219.864583222,3924.81294808)),
    DenseVector(Array(8799.16172062,9151.65917118,3935.17017405,5985.04755564)),
    DenseVector(Array(2652.33870096,4569.46205716,8197.24707007,4646.47382793)),
    DenseVector(Array(1516.56650617,7576.54110317,9437.54846944,4083.7965024)),
    DenseVector(Array(7910.99232493,2089.4966212,4482.93548739,963.171633221)),
    DenseVector(Array(2510.0660833,5004.48871099,6217.02618572,9298.36840191)),
    DenseVector(Array(3797.85513863,5056.43152967,9120.75237924,4745.25883832)),
    DenseVector(Array(3157.07920203,450.719269888,5766.92449832,1443.30412158)),
    DenseVector(Array(7095.50107308,6370.50105482,6546.33814713,6331.0302286)),
    DenseVector(Array(2949.86324508,1629.61231601,4479.37532761,240.036597772)),
    DenseVector(Array(81.6639437103,5089.56609075,7064.1466657,2828.92242805)),
    DenseVector(Array(5656.59316276,5326.46266153,6227.82697136,5247.02330409)),
    DenseVector(Array(9535.92546974,6358.60624328,3333.82027059,3734.56225841)),
    DenseVector(Array(1421.85677932,6324.8034007,1875.50810427,9887.32030456)),
    DenseVector(Array(1861.807005,689.843915471,7052.17543863,9171.05614503)),
    DenseVector(Array(9778.83969108,3735.92587647,9062.5805125,2706.2465034)),
    DenseVector(Array(1849.00060543,5569.37328538,1708.70234317,3532.23859204)),
    DenseVector(Array(341.292580631,4841.26724281,3328.99580592,1548.87171433)),
    DenseVector(Array(5667.05623399,13.0999036273,2373.81482134,4657.72003168)),
    DenseVector(Array(7629.62772941,9021.98406384,7608.8097837,6892.65551107)),
    DenseVector(Array(3913.27878604,1864.23781872,2062.23260567,7177.53629028)),
    DenseVector(Array(3606.48274613,1800.96003262,2131.530793,5405.9900662)),
    DenseVector(Array(5981.0902968,1190.12613714,4434.10156081,9237.94664753)),
    DenseVector(Array(9765.14641968,3550.17617532,3570.28678612,5441.56371696)),
    DenseVector(Array(4813.73975553,9882.9192585,4045.00768939,7426.32604672)),
    DenseVector(Array(2707.40057647,5578.70510354,5564.20765501,1769.7699925)),
    DenseVector(Array(5228.58532657,8681.51329792,6942.85184887,3066.44777398)),
    DenseVector(Array(2712.1217616,2723.47483103,2513.65412997,8251.02563282)),
    DenseVector(Array(1625.57757159,7173.53812111,5002.44219166,2792.64054759)),
    DenseVector(Array(5061.92147521,5412.21860136,9236.99517307,795.97520478)),
    DenseVector(Array(1405.82712629,7600.40550588,1040.08399204,940.494553814)),
    DenseVector(Array(9097.7211252,6019.94302671,8101.13073619,3447.23551186)),
    DenseVector(Array(8725.01575502,5716.59817232,113.019541015,1159.86952457)),
    DenseVector(Array(4038.44295645,6369.44346072,5758.27908104,7848.90557223)),
    DenseVector(Array(6963.61460238,3618.06473942,5756.10450476,1658.6720137)),
    DenseVector(Array(960.159267052,1517.9252647,2106.00331292,6677.62942566)),
    DenseVector(Array(2193.45938258,3739.57686624,4467.44901118,6567.51705866)),
    DenseVector(Array(354.887853178,3643.41267376,8946.16856313,371.694904082)),
    DenseVector(Array(8529.60708829,7849.28124482,5254.87231153,6704.46408001)),
    DenseVector(Array(3735.90618304,205.799987209,4425.80047423,141.219006271)),
    DenseVector(Array(2347.7186137,7262.85508382,6270.79869013,2951.09303806)),
    DenseVector(Array(8637.33974842,276.584070191,216.270054171,9506.0409011)),
    DenseVector(Array(6380.07013056,5224.5998374,1058.70471312,3127.46781741)),
    DenseVector(Array(9482.70716368,7566.63842081,2297.38287553,1208.22822821)),
    DenseVector(Array(2648.82561882,2483.75741319,1703.11372923,3912.04471268)),
    DenseVector(Array(2128.63976145,6424.40292606,388.466446083,9097.51355985)),
    DenseVector(Array(8740.70541939,2475.37900651,8984.18572388,3407.24980304)),
    DenseVector(Array(2042.83582816,7172.97542226,1281.62265313,9367.93338834)),
    DenseVector(Array(1491.06418163,8315.14522389,1972.91305126,4532.52047348)),
    DenseVector(Array(6905.18371679,7440.56708114,4696.80508581,1421.4665855)),
    DenseVector(Array(1812.9418899,6983.40891001,312.495068238,9672.73593647))
  )

  val labeledData: Seq[LabeledVector] = List(
    LabeledVector(778.444166282, DenseVector(Array(331.142058093,6483.19076244,4619.70390814))),
    LabeledVector(449.7585075, DenseVector(Array(6475.71300395,4815.96198805,3910.22067074))),
    LabeledVector(802.464740437, DenseVector(Array(6489.77018109,4011.41646732,639.112849635))),
    LabeledVector(611.063380203, DenseVector(Array(9150.53462565,7982.46702971,6062.64988236))),
    LabeledVector(725.207764707, DenseVector(Array(28.7504893255,3036.99727387,1916.3018766))),
    LabeledVector(561.307905913, DenseVector(Array(5436.29825501,3248.06752373,3871.96121551))),
    LabeledVector(196.671866796, DenseVector(Array(8278.69068881,7501.04020878,8617.22457157))),
    LabeledVector(242.550840101, DenseVector(Array(2569.48325832,5617.44421185,4614.0276161))),
    LabeledVector(796.064912385, DenseVector(Array(210.95163429,492.936987066,2577.17609953))),
    LabeledVector(693.904862506, DenseVector(Array(447.064920736,2540.95985944,9293.25275717))),
    LabeledVector(779.146523736, DenseVector(Array(8222.94523317,8371.72829464,8886.23473025))),
    LabeledVector(924.020888061, DenseVector(Array(5908.84052111,682.629823477,3902.02373602))),
    LabeledVector(485.732741066, DenseVector(Array(5333.98036031,4750.15895088,9489.82104397))),
    LabeledVector(867.38884566, DenseVector(Array(1390.01965862,2314.83732821,4043.83113568))),
    LabeledVector(378.53561857, DenseVector(Array(2963.72280322,1316.17129863,9758.05367052))),
    LabeledVector(195.445643481, DenseVector(Array(1992.08390142,9669.91918761,6429.70044595))),
    LabeledVector(253.409034058, DenseVector(Array(221.266040627,1072.77947126,2578.6727227))),
    LabeledVector(736.848225397, DenseVector(Array(3847.46412812,4761.11314283,8762.75466857))),
    LabeledVector(367.366733464, DenseVector(Array(6629.55316727,4621.69764511,8242.41424819))),
    LabeledVector(81.8640512918, DenseVector(Array(4939.70676415,7223.18548989,5867.85401137))),
    LabeledVector(773.047049038, DenseVector(Array(5928.72293888,8876.94384724,5993.83644822))),
    LabeledVector(705.119911651, DenseVector(Array(9557.25026009,2495.04175333,6473.92744587))),
    LabeledVector(871.239802516, DenseVector(Array(490.142555718,3717.45220585,7611.22130133))),
    LabeledVector(483.17646008, DenseVector(Array(2191.47060374,824.104401584,7315.06370318))),
    LabeledVector(268.499195679, DenseVector(Array(1760.37753417,5767.39010172,4018.63931413))),
    LabeledVector(609.046917982, DenseVector(Array(8494.1273805,9171.72621689,182.383875266))),
    LabeledVector(116.756892662, DenseVector(Array(8557.01524897,9717.36635444,5098.22365248))),
    LabeledVector(673.054176694, DenseVector(Array(3408.6004389,4536.49009712,1166.22901755))),
    LabeledVector(497.71287519, DenseVector(Array(3916.32599196,3652.19574877,1019.32553449))),
    LabeledVector(975.431472053, DenseVector(Array(5687.91786293,4014.34775931,9511.97377894))),
    LabeledVector(108.223895796, DenseVector(Array(9085.3261259,9002.34583828,407.540373894))),
    LabeledVector(750.009481034, DenseVector(Array(1611.31673664,3337.42983985,3023.14976951))),
    LabeledVector(406.279315638, DenseVector(Array(4887.83625787,2792.89921386,2938.3460598))),
    LabeledVector(671.159420683, DenseVector(Array(5901.51477485,5405.09757722,5716.00283316))),
    LabeledVector(53.4452763389, DenseVector(Array(3969.90988963,7242.63013608,2709.54104566))),
    LabeledVector(490.974619721, DenseVector(Array(7574.5677851,5777.0063577,3206.79488559))),
    LabeledVector(531.948182817, DenseVector(Array(1008.96223065,7746.17509489,6464.70142639))),
    LabeledVector(81.0829431404, DenseVector(Array(7992.00161842,4524.64361852,3144.64814952))),
    LabeledVector(880.577769088, DenseVector(Array(4890.93443423,9188.40404239,8479.08540806))),
    LabeledVector(808.94396204, DenseVector(Array(3367.30130172,4583.28473014,5086.63869783))),
    LabeledVector(927.139954788, DenseVector(Array(1961.98449889,2307.88058462,1913.70657295))),
    LabeledVector(416.928421736, DenseVector(Array(2486.72109546,485.614049171,6000.48040405))),
    LabeledVector(601.356419047, DenseVector(Array(5367.49263484,4047.44948899,6926.165125))),
    LabeledVector(233.021607069, DenseVector(Array(5359.08125419,4718.60660294,639.730735288))),
    LabeledVector(35.4979225204, DenseVector(Array(7969.47605799,9311.91591103,3679.20333051))),
    LabeledVector(374.883740243, DenseVector(Array(3547.55907761,4246.44284296,934.265840783))),
    LabeledVector(708.42349618, DenseVector(Array(8355.7802877,7033.48081111,9788.68512851))),
    LabeledVector(453.761783905, DenseVector(Array(1075.68075496,7562.08231296,7810.08662226))),
    LabeledVector(112.519034993, DenseVector(Array(2254.65318973,5373.21522062,5669.59434103))),
    LabeledVector(650.830856178, DenseVector(Array(5221.16981217,8598.38884802,6649.80426476))),
    LabeledVector(486.320508346, DenseVector(Array(5128.72464684,9745.76344001,5579.11130285))),
    LabeledVector(919.457385209, DenseVector(Array(1734.32401339,9152.25363895,8414.54849755))),
    LabeledVector(558.868893871, DenseVector(Array(5311.50467423,1326.5504308,8398.39530249))),
    LabeledVector(563.054620521, DenseVector(Array(6286.10217324,7925.69933054,4184.36661684))),
    LabeledVector(783.017179464, DenseVector(Array(66.2867506867,465.89068491,7542.48750061))),
    LabeledVector(334.266926426, DenseVector(Array(8104.40741947,2409.11701656,6139.1139685))),
    LabeledVector(3.32571955769, DenseVector(Array(6889.990812,2136.99587418,6162.94314791))),
    LabeledVector(660.832288782, DenseVector(Array(6550.4560833,4787.69005418,5590.96669067))),
    LabeledVector(434.18057246, DenseVector(Array(7203.91348206,2179.48170738,4883.50567632))),
    LabeledVector(656.882403394, DenseVector(Array(9882.2072236,1455.38960902,2399.49468809))),
    LabeledVector(656.689778244, DenseVector(Array(3384.07071963,3276.38529389,3235.40039515))),
    LabeledVector(830.448197202, DenseVector(Array(8707.80203032,144.400305454,3112.71986428))),
    LabeledVector(542.669869697, DenseVector(Array(9331.87479819,1190.85361545,8484.20343348))),
    LabeledVector(317.662412137, DenseVector(Array(1658.51697253,3793.40760497,8349.3889907))),
    LabeledVector(29.3075532818, DenseVector(Array(855.020480866,5749.31139559,8225.97560589))),
    LabeledVector(479.405289422, DenseVector(Array(3654.33243337,5901.6047935,1488.91437867))),
    LabeledVector(508.146357144, DenseVector(Array(8494.4197585,3994.14538194,9687.81481292))),
    LabeledVector(767.007300078, DenseVector(Array(6535.07682278,8733.12696216,3068.88486793))),
    LabeledVector(855.206289321, DenseVector(Array(1236.9847348,8941.34133975,7143.7530963))),
    LabeledVector(869.289062034, DenseVector(Array(8332.37555822,1790.71012894,4412.85038698))),
    LabeledVector(957.789404232, DenseVector(Array(465.140239109,5258.45147656,6971.10791677))),
    LabeledVector(489.870642583, DenseVector(Array(7178.46712745,7746.92278423,5246.11895356))),
    LabeledVector(618.350538327, DenseVector(Array(4241.08590224,8072.85968776,4854.1493064))),
    LabeledVector(424.568755547, DenseVector(Array(3334.32767387,3764.5427327,6446.02103835))),
    LabeledVector(281.77418248, DenseVector(Array(883.447383917,1743.17255188,1996.54981686))),
    LabeledVector(190.079777371, DenseVector(Array(8875.15514846,7115.88166438,1048.43618779))),
    LabeledVector(967.696549175, DenseVector(Array(5472.92266022,4366.96410545,7547.0670318))),
    LabeledVector(435.895158291, DenseVector(Array(1359.15201683,1688.8183654,3492.9635947))),
    LabeledVector(685.829292795, DenseVector(Array(3293.37058708,9488.43061853,8765.61086317))),
    LabeledVector(613.124902556, DenseVector(Array(8673.14019666,3169.54523772,6865.48662294))),
    LabeledVector(652.425891645, DenseVector(Array(7465.39466138,6122.64479576,4238.48952122))),
    LabeledVector(263.719855591, DenseVector(Array(2569.1069326,7802.63436093,4001.25716696))),
    LabeledVector(953.071135874, DenseVector(Array(3939.3145721,1143.37969073,8460.88782006))),
    LabeledVector(591.397957117, DenseVector(Array(5548.98476615,4132.22214688,7993.81430111))),
    LabeledVector(798.016675914, DenseVector(Array(8445.52134701,7376.20325543,7623.92814928))),
    LabeledVector(334.506225879, DenseVector(Array(5170.62821737,9725.44743794,3790.17949824))),
    LabeledVector(596.447386903, DenseVector(Array(2150.89539935,6446.78284738,5836.03693171))),
    LabeledVector(614.287019652, DenseVector(Array(711.903667638,8129.39997661,2718.19075277))),
    LabeledVector(878.708345596, DenseVector(Array(6335.95114913,9295.78597748,7122.63605931))),
    LabeledVector(595.884274932, DenseVector(Array(5670.0221531,343.147868922,4545.46446984))),
    LabeledVector(463.911893977, DenseVector(Array(6248.61183947,7146.72197399,9151.79670357))),
    LabeledVector(588.790728483, DenseVector(Array(8203.27400147,9322.03851103,9204.15780379))),
    LabeledVector(670.178843317, DenseVector(Array(9979.89864198,5552.88757487,566.66032997))),
    LabeledVector(720.342828849, DenseVector(Array(3307.79764509,8413.77520856,4024.26420366))),
    LabeledVector(17.2918238379, DenseVector(Array(9601.76034813,2030.90520403,9149.25922591))),
    LabeledVector(775.407603499, DenseVector(Array(3819.82035314,8463.37088936,1613.84733334))),
    LabeledVector(807.068324397, DenseVector(Array(6962.26823268,4254.28662452,7061.31063068))),
    LabeledVector(205.008519121, DenseVector(Array(8024.66418641,9780.34755432,442.266734451))),
    LabeledVector(607.954942321, DenseVector(Array(8412.86692569,8963.30413497,694.751557979)))
  )

  val badDataCase1: Seq[Vector] = List(
    DenseVector(Array(Double.PositiveInfinity, 10.0, 10.0, 10.0))
  )

  val badDataCase2: Seq[Vector] = List(
    DenseVector(Array(Double.NaN, 10.0, 10.0, 10.0))
  )

  val badDataLabeledCase1: Seq[LabeledVector] = List(
    LabeledVector(3.0, DenseVector(Array(Double.PositiveInfinity, 10.0, 10.0, 10.0, 10.0)))
  )

  val badDataLabeledCase2: Seq[LabeledVector] = List(
    LabeledVector(3.0, DenseVector(Array(Double.NaN, 10.0, 10.0, 10.0, 10.0)))
  )

  val zeroData =  List(
    DenseVector(Array(0.0, 0.0, 0.0, 0.0))
  )

  val zeroLabeledData: Seq[LabeledVector] = List(
    LabeledVector(3.0, DenseVector(Array(0.0, 0.0, 0.0, 0.0)))
  )
}
