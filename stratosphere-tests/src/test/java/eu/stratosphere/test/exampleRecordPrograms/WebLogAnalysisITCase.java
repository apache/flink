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

import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.Job;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.record.relational.WebLogAnalysis;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.util.TestBase;

@RunWith(Parameterized.class)
public class WebLogAnalysisITCase extends TestBase {

	private static final Log LOG = LogFactory.getLog(WebLogAnalysisITCase.class);
	
	protected String docsPath = null;
	protected String ranksPath = null;
	protected String visitsPath = null;
	protected String resultPath = null;

	String docs = 
		"url_10|volutpat magna quis consectetuer volutpat ad erat editors exerci oscillations euismod volutpat Lorem convectionullamcorper Lorem volutpat enim tation elit |\n" + 
		"url_11|adipiscing enim diam ex tincidunt tincidunt nonummy volutpat minim euismod volutpat suscipit ex sed laoreet aliquip quis diam tincidunt wisi diam elit sed ut minim ad nonummy amet volutpat nostrud erat |\n" + 
		"url_12|ut nostrud adipiscing adipiscing ipsum nonummy amet volutpat volutpat sit enim Ut amet |\n" + 
		"url_13|euismod sit adipiscing ex suscipit ea veniam tincidunt laoreet nibh editors ullamcorper consectetuer convection commodo sed nostrud ea ex ullamcorper dolor dolore ad diam dolore amet ut tincidunt nonummy euismod enim ullamcorper tincidunt dolor sit volutpat dolor tincidunt aliquam nisl tation ullamcorper sed consectetuer sit sit laoreet ex |\n" + 
		"url_14|ad magna ipsum nonummy aliquip dolore aliquam veniam lobortis nostrud aliquip nibh amet aliquam editors magna aliquam volutpat nonummy sed tation erat adipiscing nostrud magna ut sit dolore volutpat laoreet nisl wisi Ut veniam nibh laoreet ad nostrud ut aliquip |\n" + 
		"url_15|enim ipsum veniam ex editor Lorem elit laoreet exerci ea wisi oscillations convection euismod euismod diam ut euismod ad Lorem ut Ut tation wisi diam suscipit nibh nostrud minim dolor |\n" + 
		"url_16|tation ad sed veniam lobortis editor nonummy Ut ea ipsum aliquip dolore ut laoreet tation ad ut Lorem ipsum minim nostrud quis Lorem enim |\n" + 
		"url_17|veniam editor veniam Lorem aliquam ipsum amet ut convection veniam enim commodo ad ex magna nibh Ut ex sed ut nostrud amet volutpat nibh Ut wisi ea laoreet Lorem amet minim ullamcorper tincidunt veniam laoreet laoreet |\n" + 
		"url_18|veniam diam tincidunt ullamcorper adipiscing oscillations adipiscing aliquip quis nibh suscipit ex sit sit wisi diam aliquam aliquip ea diam euismod ad erat ut ipsum lobortis ea exerci |\n" + 
		"url_19|adipiscing nisl oscillations ea editors tincidunt tation convection tincidunt tincidunt ut wisi tincidunt ut ut Ut nibh aliquam laoreet exerci enim ea nibh erat nonummy |\n" + 
		"url_20|ut diam aliquip ipsum laoreet elit volutpat suscipit nostrud convection tation nisl suscipit nonummy tation tation ut enim dolor nisl magna aliquam enim consectetuer sed tincidunt quis amet elit |\n" + 
		"url_21|tation ad exerci volutpat tincidunt sit ullamcorper oscillations ut minim sed diam Lorem adipiscing |\n" + 
		"url_22|sit exerci ad ut minim convection ad ea dolore Lorem ipsum amet sit wisi sed ullamcorper ipsum enim aliquam tincidunt elit nonummy laoreet laoreet dolore tation ullamcorper commodo Ut elit sit minim suscipit nisl laoreet |\n" + 
		"url_23|Lorem nibh ea ea ex ut tation euismod tincidunt lobortis enim ullamcorper euismod amet magna sit erat enim editor diam dolor volutpat diam nisl ut erat commodo amet veniam consectetuer ex commodo sed magna ea erat ullamcorper dolor sed diam enim euismod |\n" + 
		"url_24|nisl dolor magna editors ad consectetuer veniam commodo Ut dolor wisi dolore amet dolore dolore volutpat convection Ut oscillations |\n" + 
		"url_25|ut euismod quis ad nostrud volutpat dolor wisi oscillations nostrud sed nonummy wisi exerci convection sed erat nostrud quis editors Ut nostrud consectetuer aliquip exerci ut Ut tincidunt aliquam suscipit erat |\n" + 
		"url_26|nisl elit ea minim aliquam dolor consectetuer consectetuer quis Ut convection laoreet sit enim nostrud sed dolor magna dolor elit adipiscing |\n" + 
		"url_27|nisl euismod ipsum ex adipiscing erat euismod diam quis oscillations aliquip nisl ut sit dolor wisi enim tincidunt amet ullamcorper adipiscing nibh Ut volutpat sed nonummy ex ea wisi exerci aliquam elit Ut aliquip nostrud ad nibh ut sit suscipit ut commodo wisi Lorem nibh |\n" + 
		"url_28|laoreet sed editor dolor diam convection sed diam exerci laoreet oscillations consectetuer ullamcorper suscipit ut editors quis commodo editor veniam nibh ea diam ex magna ea elit sit sed nibh lobortis consectetuer erat erat sit minim ea ad ea nisl magna volutpat ut |\n" + 
		"url_29|consectetuer convection amet diam erat euismod erat editor oscillations ipsum tation tation editors minim wisi tation quis adipiscing euismod nonummy erat ut diam suscipit tincidunt ut consectetuer Lorem editor sit quis euismod veniam tincidunt aliquam ut veniam adipiscing magna Ut ut dolore euismod minim veniam |";

	String ranks = 
		"77|url_10|51|\n" + "7|url_11|24|\n" + "19|url_12|28|\n" + "13|url_13|50|\n" + 
		"81|url_14|41|\n" + "87|url_15|28|\n" + "78|url_16|29|\n" + "70|url_17|7|\n" + 
		"91|url_18|38|\n" + "99|url_19|11|\n" + "37|url_20|46|\n" + "64|url_21|46|\n" + 
		"25|url_22|14|\n" + "6|url_23|12|\n" + "87|url_24|39|\n" + "0|url_25|27|\n" + 
		"97|url_26|27|\n" + "9|url_27|54|\n" + "59|url_28|41|\n" + "40|url_29|11|";

	String visits = 
		"112.99.215.248|url_20|2011-5-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "189.69.147.166|url_19|2009-8-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"83.7.97.153|url_21|2010-3-20|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "215.57.124.59|url_10|2008-1-26|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"174.176.121.10|url_25|2007-12-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "66.225.121.11|url_16|2007-10-8|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"57.11.211.126|url_14|2010-12-6|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "129.127.115.129|url_28|2009-2-5|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"137.249.175.115|url_27|2011-5-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "101.92.9.112|url_12|2012-1-10|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"113.108.207.12|url_24|2011-3-20|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "179.141.87.220|url_18|2007-10-3|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"46.10.118.101|url_20|2009-7-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "91.67.84.155|url_20|2012-7-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"102.142.34.164|url_22|2010-7-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "203.67.94.61|url_14|2007-7-27|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"229.121.13.143|url_19|2010-4-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "209.154.189.12|url_16|2011-4-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"57.113.29.197|url_13|2008-9-23|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "132.90.139.12|url_16|2007-10-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"101.21.109.146|url_29|2009-2-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "171.69.34.255|url_29|2009-2-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"11.114.192.13|url_25|2008-9-8|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "37.188.212.141|url_15|2011-1-21|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"102.40.251.47|url_26|2010-7-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "136.85.230.245|url_17|2010-8-12|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"68.96.194.82|url_29|2007-10-12|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "62.126.40.210|url_29|2011-2-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"12.81.184.191|url_11|2008-1-12|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "12.232.205.52|url_18|2008-1-26|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"227.83.79.30|url_25|2008-6-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "88.222.17.152|url_21|2007-10-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"55.87.23.63|url_22|2008-8-16|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "54.41.77.25|url_18|2012-12-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"203.83.250.233|url_10|2009-4-5|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "42.13.134.196|url_15|2012-9-23|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"224.51.182.133|url_27|2008-11-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "180.209.212.98|url_27|2008-6-27|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"29.173.47.29|url_24|2011-9-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "251.153.164.73|url_18|2008-3-6|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"195.164.244.58|url_23|2010-12-21|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "96.167.244.51|url_18|2007-2-10|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"131.169.48.126|url_19|2009-6-10|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "171.151.109.83|url_12|2011-6-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"150.87.176.48|url_22|2008-10-3|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "56.172.201.54|TPCHQuery3ITCaseurl_25|2012-11-3|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"175.241.83.44|url_13|2008-2-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "105.222.1.251|url_18|2012-7-6|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"195.117.130.95|url_10|2012-4-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "10.139.236.176|url_14|2011-4-9|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"207.100.168.191|url_22|2007-5-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "196.129.126.177|url_25|2008-4-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"170.239.116.200|url_27|2010-3-15|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "99.79.118.85|url_13|2008-3-12|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"112.65.132.135|url_15|2010-8-3|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "109.15.235.196|url_14|2012-8-25|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"240.201.229.137|url_21|2008-9-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "74.53.245.171|url_18|2007-6-19|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"158.230.101.250|url_17|2011-4-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "198.197.213.210|url_11|2012-5-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"1.54.145.90|url_11|2009-6-16|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "2.236.14.86|url_13|2007-9-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"203.180.186.150|url_16|2010-2-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "135.83.247.101|url_22|2007-12-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"54.214.72.16|url_12|2007-5-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "238.115.186.34|url_10|2007-5-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"6.251.55.92|url_26|2007-5-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "196.96.11.111|url_16|2007-11-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"209.40.86.220|url_24|2009-5-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "130.208.66.140|url_19|2011-9-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"176.157.170.178|url_27|2008-12-12|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "103.4.119.128|url_15|2010-6-19|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"31.181.160.159|url_12|2009-8-10|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "166.238.60.79|url_23|2009-2-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"64.165.114.139|url_19|2007-9-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "11.45.238.55|url_26|2011-8-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"189.121.92.228|url_18|2008-1-16|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "137.114.181.208|url_14|2007-6-15|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"165.194.234.136|url_18|2012-3-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "210.145.85.234|url_10|2007-9-3|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"227.37.135.138|url_21|2010-5-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "39.87.120.86|url_25|2007-2-6|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"107.144.159.246|url_11|2010-7-6|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "160.38.166.236|url_15|2009-6-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"79.44.39.160|url_10|2008-12-19|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "143.52.62.175|url_19|2011-12-8|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"68.54.137.193|url_19|2007-6-6|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "199.137.128.158|url_27|2008-4-26|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"10.0.80.61|url_10|2011-3-15|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "4.30.48.164|url_29|2007-2-26|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"136.29.50.223|url_12|2008-3-5|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "83.101.200.110|url_23|2009-1-26|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"11.149.83.246|url_29|2010-9-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "154.119.20.134|url_14|2011-1-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"191.124.167.123|url_15|2009-6-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "27.158.167.38|url_11|2011-5-16|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"16.197.156.206|url_16|2009-2-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "222.150.206.233|url_20|2012-2-11|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"198.59.29.225|url_21|2007-2-25|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "182.91.244.40|url_18|2010-5-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"23.110.45.72|url_28|2008-6-23|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "94.241.8.36|url_11|2007-7-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"226.223.202.210|url_17|2007-10-15|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "73.16.194.132|url_14|2012-7-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"195.45.98.110|url_13|2012-2-23|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "189.123.80.178|url_29|2011-1-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"190.134.174.18|url_23|2011-12-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "133.54.252.245|url_13|2011-3-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"244.29.167.182|url_18|2012-10-11|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "125.32.251.232|url_26|2010-4-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"74.139.30.20|url_16|2012-2-8|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "100.73.180.209|url_29|2010-4-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"133.155.158.147|url_16|2011-11-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "185.53.212.102|url_15|2010-12-19|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"154.189.125.92|url_27|2007-10-5|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "12.209.166.107|url_26|2007-5-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"106.99.152.22|url_25|2011-12-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "37.120.32.53|url_22|2008-12-8|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"29.17.125.185|url_12|2011-8-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "233.125.12.14|url_19|2011-2-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"241.78.212.114|url_25|2007-12-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "153.183.247.90|url_18|2011-9-23|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"245.43.17.177|url_15|2012-4-21|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "55.13.54.220|url_24|2008-11-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"104.149.82.217|url_10|2011-9-26|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "36.15.167.145|url_24|2008-1-9|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"145.149.81.203|url_20|2008-5-27|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "68.89.4.115|url_18|2007-7-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"236.88.134.53|url_15|2010-5-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "180.140.58.209|url_29|2012-8-5|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"138.46.241.237|url_17|2010-11-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "254.217.79.58|url_29|2007-1-11|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"142.184.197.136|url_10|2007-11-3|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "216.118.94.220|url_12|2007-2-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"53.99.54.0|url_14|2008-3-9|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "95.151.221.37|url_28|2009-5-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"46.110.43.78|url_27|2007-11-9|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "172.96.203.180|url_11|2012-1-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"22.188.231.143|url_21|2007-10-23|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "216.46.80.239|url_22|2009-9-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"103.213.7.86|url_26|2007-12-25|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "186.204.3.180|url_27|2012-1-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"129.197.116.192|url_10|2012-5-1|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "8.164.181.242|url_24|2008-11-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"145.52.12.132|url_28|2012-8-6|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "20.22.62.146|url_11|2007-4-27|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"233.255.32.151|url_19|2008-6-25|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "31.233.226.148|url_24|2011-8-4|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"57.130.120.117|url_25|2008-12-9|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "29.98.249.152|url_21|2011-8-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"25.187.66.122|url_20|2008-12-17|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "236.205.44.215|url_26|2008-9-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"18.40.99.157|url_23|2012-1-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "204.221.88.206|url_24|2012-11-8|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"199.5.94.240|url_24|2011-9-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "57.175.34.24|url_14|2009-12-10|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"130.25.116.220|url_10|2007-6-5|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "134.174.12.250|url_22|2011-6-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"162.185.114.212|url_13|2011-8-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "8.113.137.95|url_14|2011-6-22|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"176.253.5.99|url_12|2010-4-12|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "3.110.24.0|url_12|2009-6-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"159.128.152.207|url_17|2012-2-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "104.145.150.244|url_10|2012-2-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"203.19.224.207|url_12|2012-9-24|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "225.163.125.108|url_20|2010-4-8|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"197.137.29.71|url_21|2007-10-25|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "29.207.144.144|url_12|2012-7-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"139.222.222.113|url_14|2010-4-15|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "169.225.20.141|url_29|2012-2-18|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"137.86.141.104|url_26|2012-3-10|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "193.100.2.122|url_24|2011-12-15|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"20.105.209.161|url_13|2010-2-7|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "116.81.52.207|url_11|2007-4-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"141.88.224.116|url_19|2009-4-12|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "72.198.58.37|url_26|2012-2-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"13.37.92.90|url_22|2011-5-16|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "191.163.173.142|url_24|2012-5-2|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"183.57.175.105|url_27|2007-8-13|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "230.212.34.87|url_10|2010-12-21|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + 
		"133.150.217.96|url_24|2012-7-27|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n" + "62.254.96.239|url_24|2011-5-15|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|";

	String expected = "87|url_24|39\n" + "59|url_28|41\n";

	public WebLogAnalysisITCase(Configuration config) {
		super(config);
	}

	@Override
	protected String getJarFilePath() {
		return null;
	}

	@Override
	protected void preSubmit() throws Exception {
		
		docsPath   = getFilesystemProvider().getTempDirPath() + "/docs";
		ranksPath  = getFilesystemProvider().getTempDirPath() + "/ranks";
		visitsPath = getFilesystemProvider().getTempDirPath() + "/visits";
		resultPath = getFilesystemProvider().getTempDirPath() + "/results"; 
			
		String[] splits = splitInputString(docs, '\n', 4);
		getFilesystemProvider().createDir(docsPath);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(docsPath + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Docs Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

		splits = splitInputString(ranks, '\n', 4);
		getFilesystemProvider().createDir(ranksPath);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(ranksPath + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Ranks Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

		splits = splitInputString(visits, '\n', 4);
		getFilesystemProvider().createDir(visitsPath);
		for (int i = 0; i < splits.length; i++) {
			getFilesystemProvider().createFile(visitsPath + "/part_" + i + ".txt", splits[i]);
			LOG.debug("Visits Part " + (i + 1) + ":\n>" + splits[i] + "<");
		}

	}

	@Override
	protected JobGraph getJobGraph() throws Exception {

		WebLogAnalysis relOLAP = new WebLogAnalysis();
		Job plan = relOLAP.createJob(config.getString("WebLogAnalysisTest#NoSubtasks", "1"),
			getFilesystemProvider().getURIPrefix()+docsPath, 
			getFilesystemProvider().getURIPrefix()+ranksPath, 
			getFilesystemProvider().getURIPrefix()+visitsPath, 
			getFilesystemProvider().getURIPrefix()+resultPath);

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	@Override
	protected void postSubmit() throws Exception {

		// Test results
		compareResultsByLinesInMemory(expected, resultPath);

		// clean up hdfs
		getFilesystemProvider().delete(docsPath, true);
		getFilesystemProvider().delete(ranksPath, true);
		getFilesystemProvider().delete(visitsPath, true);
		getFilesystemProvider().delete(resultPath, true);
		
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		Configuration config = new Configuration();
		config.setInteger("WebLogAnalysisTest#NoSubtasks", 4);
		tConfigs.add(config);

		return toParameterList(tConfigs);
	}

	private String[] splitInputString(String inputString, char splitChar, int noSplits) {

		String splitString = inputString.toString();
		String[] splits = new String[noSplits];
		int partitionSize = (splitString.length() / noSplits) - 2;

		// split data file and copy parts
		for (int i = 0; i < noSplits - 1; i++) {
			int cutPos = splitString.indexOf(splitChar, (partitionSize < splitString.length() ? partitionSize
				: (splitString.length() - 1)));
			splits[i] = splitString.substring(0, cutPos) + "\n";
			splitString = splitString.substring(cutPos + 1);
		}
		splits[noSplits - 1] = splitString;

		return splits;

	}

}
