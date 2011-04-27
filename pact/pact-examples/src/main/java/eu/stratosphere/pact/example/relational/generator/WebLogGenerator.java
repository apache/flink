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

		if (args.length != 4) {
			if (args.length == 0 || args[0].equals("-h")
					|| args[0].equals("--help")) {
				// Show help
				System.out.println("Usage:");
				System.out.println("1:\tWith parameters");
				System.out.println("\t<Generator> "
						+ "[noDocuments] [noVisits] [outPath] [noFiles]");
				System.out.println("2:\tDefault parameters");
				System.out.println("\t<Generator> -d");
				return;
			} else if (args[0].equals("-d")) {
				// Default values
				args = new String[4];
				args[0] = "1000"; // number of documents
				args[1] = "10000"; // number of visits
				args[2] = "/tmp/stratosphere/"; // path
				args[3] = "1"; // number of files
			}
		}

		String[] filterKWs = { "editors", "oscillations", "convection" };

		String[] words = { "Lorem", "ipsum", "dolor", "sit", "amet",
				"consectetuer", "adipiscing", "elit", "sed", "diam", "nonummy",
				"nibh", "euismod", "tincidunt", "ut", "laoreet", "dolore",
				"magna", "aliquam", "erat", "volutpat", "Ut", "wisi", "enim",
				"ad", "minim", "veniam", "quis", "nostrud", "exerci", "tation",
				"ullamcorper", "suscipit", "lobortis", "nisl", "ut", "aliquip",
				"ex", "ea", "commodo" };

		int noDocs = Integer.parseInt(args[0]);
		int noVisits = Integer.parseInt(args[1]);

		String path = args[2];
		int noFiles = Integer.parseInt(args[3]);

		System.out.println("Generating documents files...");
		genDocs(noDocs, noFiles, filterKWs, words, path + "docs_");
		System.out.println("Generating ranks files...");
		genRanks(noDocs, noFiles, path + "ranks_");
		System.out.println("Generating visits files...");
		genVisits(noVisits, noDocs, noFiles, path + "visits_");

		System.out.println("Done!");
	}

	/**
	 * Generates the files for the documents relation. The entries apply the
	 * following format: <br />
	 * <code>URL | Content</code>
	 * 
	 * @param noDocs
	 *            Number of entries for the documents relation
	 * @param noFiles
	 *            Number of files for the documents relation
	 * @param filterKeyWords
	 *            A list of keywords that should be contained
	 * @param words
	 *            A list of words to fill the entries
	 * @param path
	 *            Output path for the documents relation
	 */
	public static void genDocs(int noDocs, int noFiles,
			String[] filterKeyWords, String[] words, String path) {

		Random rand = new Random(Calendar.getInstance().getTimeInMillis());
		int fileId = 0;
		int docsPerFile = (noDocs / noFiles) + 1;
		int docsInFile = 0;

		try {
			FileWriter fw = new FileWriter(path + (fileId++));

			for (int i = 0; i < noDocs; i++) {

				int wordsInDoc = rand.nextInt(40) + 10;
				// URL
				StringBuilder doc = new StringBuilder("url_" + i + "|");
				for (int j = 0; j < wordsInDoc; j++) {
					if (rand.nextDouble() > 0.98) {
						// Approx. every 50th word is a keyword
						doc.append(filterKeyWords[rand
								.nextInt(filterKeyWords.length)] + " ");
					} else {
						// Fills up the docs file(s) with random words
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

	/**
	 * Generates the files for the ranks relation. The ranks entries apply the
	 * following format: <br />
	 * <code>Rank | URL | Average Duration |\n</code>
	 * 
	 * @param noDocs
	 *            Number of entries in the documents relation
	 * @param noFiles
	 *            Number of files for the ranks relation
	 * @param path
	 *            Output path for the ranks relation
	 */
	public static void genRanks(int noDocs, int noFiles, String path) {

		Random rand = new Random(Calendar.getInstance().getTimeInMillis());
		int fileId = 0;
		int docsPerFile = (noDocs / noFiles) + 1;
		int docsInFile = 0;

		try {
			FileWriter fw = new FileWriter(path + (fileId++));

			for (int i = 0; i < noDocs; i++) {

				// Rank
				StringBuilder rank = new StringBuilder(rand.nextInt(100) + "|");
				// URL
				rank.append("url_" + i + "|");
				// Average duration
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

	/**
	 * Generates the files for the visits relation. The visits entries apply the
	 * following format:<br />
	 * <code>IP Address | URL | Date (YYYY-MM-DD) | Misc. Data (e.g. User-Agent) |\n</code>
	 * 
	 * @param noVisits
	 *            Number of entries for the visits relation
	 * @param noDocs
	 *            Number of entries in the documents relation
	 * @param noFiles
	 *            Number of files for the visits relation
	 * @param path
	 *            Output path for the visits relation
	 */
	public static void genVisits(int noVisits, int noDocs, int noFiles, String path) {

		Random rand = new Random(Calendar.getInstance().getTimeInMillis());
		int fileId = 0;
		int visitsPerFile = (noVisits / noFiles) + 1;
		int visitsInFile = 0;

		try {
			FileWriter fw = new FileWriter(path + (fileId++));

			for (int i = 0; i < noVisits; i++) {

				int year = 2000 + rand.nextInt(10); // yearFilter 3
				int month = rand.nextInt(12) + 1; // month between 1 and 12
				int day = rand.nextInt(27) + 1; // day between 1 and 28

				// IP address
				StringBuilder visit = new StringBuilder(rand.nextInt(256) + "."
						+ rand.nextInt(256) + "." + rand.nextInt(256) + "."
						+ rand.nextInt(256) + "|");
				// URL
				visit.append("url_" + rand.nextInt(noDocs) + "|");
				// Date (format: YYYY-MM-DD)
				visit.append(year + "-" + month + "-" + day + "|");
				// Miscellaneous data, e.g. User-Agent
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
