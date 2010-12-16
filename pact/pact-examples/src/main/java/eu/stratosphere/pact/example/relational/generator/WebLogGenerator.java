/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.relational.generator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

public class WebLogGenerator {

	public static void main(String[] args) {

		String[] filterKWs = { "cloud", "atomic", "cosmic", "universe", "chromospheres", "statistics", "resolution",
			"theory", "data", "extraterrestrial", "atoms", "kinematics" };
		String[] words = { "Lorem", "ipsum", "dolor", "sit", "amet", "consectetuer", "adipiscing", "elit", "sed",
			"diam", "nonummy", "nibh", "euismod", "tincidunt", "ut", "laoreet", "dolore", "magna", "aliquam", "erat",
			"volutpat", "Ut", "wisi", "enim", "ad", "minim", "veniam", "quis", "nostrud", "exerci", "tation",
			"ullamcorper", "suscipit", "lobortis", "nisl", "ut", "aliquip", "ex", "ea", "commodo" };
		int noDocs = 1000000;
		int noVisits = 100000000;

		genDocs(noDocs, 64, filterKWs, words, "/tmp/testdata/docs_");
		genRanks(noDocs, 64, "/tmp/testdata/ranks_");
		genVisits(noVisits, noDocs, 64, "/tmp/testdata/visits_");

	}

	public static void genDocs(int noDocs, int noFiles, String[] filterKeyWords, String[] words, String path) {

		Random rand = new Random(Calendar.getInstance().getTimeInMillis());
		int fileId = 0;
		int docsPerFile = (noDocs / noFiles) + 1;
		int docsInFile = 0;

		try {
			FileWriter fw = new FileWriter(path + (fileId++));

			for (int i = 0; i < noDocs; i++) {

				int wordsInDoc = rand.nextInt(40) + 10;
				StringBuilder doc = new StringBuilder("url_" + i + "|");
				for (int j = 0; j < wordsInDoc; j++) {
					if (rand.nextDouble() > 0.98) {
						doc.append(filterKeyWords[rand.nextInt(filterKeyWords.length)] + " ");
					} else {
						doc.append(words[rand.nextInt(words.length)] + " ");
					}
				}
				doc.append("|\n");

				fw.write(doc.toString());

				docsInFile++;
				if (docsInFile == docsPerFile) {
					fw.close();
					fw = new FileWriter(path + (fileId++));
					docsInFile = 0;
				}
			}
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void genRanks(int noDocs, int noFiles, String path) {

		Random rand = new Random(Calendar.getInstance().getTimeInMillis());
		int fileId = 0;
		int docsPerFile = (noDocs / noFiles) + 1;
		int docsInFile = 0;

		try {
			FileWriter fw = new FileWriter(path + (fileId++));

			for (int i = 0; i < noDocs; i++) {

				StringBuilder rank = new StringBuilder("url_" + i + "|");

				rank.append(rand.nextInt(100) + "|");
				rank.append(rand.nextInt(10) + rand.nextInt(50) + "|\n");

				fw.write(rank.toString());

				docsInFile++;
				if (docsInFile == docsPerFile) {
					fw.close();
					fw = new FileWriter(path + (fileId++));
					docsInFile = 0;
				}
			}
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void genVisits(int noVisits, int noDocs, int noFiles, String path) {

		Random rand = new Random(Calendar.getInstance().getTimeInMillis());
		int fileId = 0;
		int visitsPerFile = (noVisits / noFiles) + 1;
		int visitsInFile = 0;

		try {
			FileWriter fw = new FileWriter(path + (fileId++));

			for (int i = 0; i < noVisits; i++) {

				StringBuilder visit = new StringBuilder(rand.nextInt(256) + "." + rand.nextInt(256) + "."
					+ rand.nextInt(256) + "." + rand.nextInt(256) + "|");
				visit.append("url_" + rand.nextInt(noDocs) + "|");
				visit.append("0" + rand.nextInt(10) + "/0" + rand.nextInt(7) + "/2010|");
				visit.append("0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n");

				fw.write(visit.toString());

				visitsInFile++;
				if (visitsInFile == visitsPerFile) {
					fw.close();
					fw = new FileWriter(path + (fileId++));
					visitsInFile = 0;
				}
			}
			fw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
