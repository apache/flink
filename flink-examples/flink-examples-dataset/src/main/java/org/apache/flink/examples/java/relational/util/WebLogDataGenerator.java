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

package org.apache.flink.examples.java.relational.util;

import org.apache.flink.examples.java.relational.WebLogAnalysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

/** Data generator for the {@link WebLogAnalysis} example program. */
public class WebLogDataGenerator {

    /**
     * Main method to generate data for the {@link WebLogAnalysis} example program.
     *
     * <p>The generator creates to files:
     *
     * <ul>
     *   <li><code>{tmp.dir}/documents</code> for the web documents
     *   <li><code>{tmp.dir}/ranks</code> for the ranks of the web documents
     *   <li><code>{tmp.dir}/visits</code> for the logged visits of web documents
     * </ul>
     *
     * @param args
     *     <ol>
     *       <li>Int: Number of web documents
     *       <li>Int: Number of visits
     *     </ol>
     */
    public static void main(String[] args) {

        // parse parameters
        if (args.length < 2) {
            System.out.println("WebLogDataGenerator <numberOfDocuments> <numberOfVisits>");
            System.exit(1);
        }

        int noDocs = Integer.parseInt(args[0]);
        int noVisits = Integer.parseInt(args[1]);

        String[] filterKWs = {"editors", "oscillations", "convection"};

        String[] words = {
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "sed",
            "diam",
            "nonummy",
            "nibh",
            "euismod",
            "tincidunt",
            "ut",
            "laoreet",
            "dolore",
            "magna",
            "aliquam",
            "erat",
            "volutpat",
            "Ut",
            "wisi",
            "enim",
            "ad",
            "minim",
            "veniam",
            "quis",
            "nostrud",
            "exerci",
            "tation",
            "ullamcorper",
            "suscipit",
            "lobortis",
            "nisl",
            "ut",
            "aliquip",
            "ex",
            "ea",
            "commodo"
        };

        final String outPath = System.getProperty("java.io.tmpdir");

        System.out.println("Generating documents files...");
        genDocs(noDocs, filterKWs, words, outPath + "/documents");
        System.out.println("Generating ranks files...");
        genRanks(noDocs, outPath + "/ranks");
        System.out.println("Generating visits files...");
        genVisits(noVisits, noDocs, outPath + "/visits");

        System.out.println("Done!");
    }

    /**
     * Generates the files for the documents relation. The entries apply the following format: <br>
     * <code>URL | Content</code>
     *
     * @param noDocs Number of entries for the documents relation
     * @param filterKeyWords A list of keywords that should be contained
     * @param words A list of words to fill the entries
     * @param path Output path for the documents relation
     */
    private static void genDocs(int noDocs, String[] filterKeyWords, String[] words, String path) {

        Random rand = new Random(Calendar.getInstance().getTimeInMillis());

        try (BufferedWriter fw = new BufferedWriter(new FileWriter(path))) {
            for (int i = 0; i < noDocs; i++) {

                int wordsInDoc = rand.nextInt(40) + 10;
                // URL
                StringBuilder doc = new StringBuilder("url_" + i + "|");
                for (int j = 0; j < wordsInDoc; j++) {
                    if (rand.nextDouble() > 0.9) {
                        // Approx. every 10th word is a keyword
                        doc.append(filterKeyWords[rand.nextInt(filterKeyWords.length)] + " ");
                    } else {
                        // Fills up the docs file(s) with random words
                        doc.append(words[rand.nextInt(words.length)] + " ");
                    }
                }
                doc.append("|\n");

                fw.write(doc.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates the files for the ranks relation. The ranks entries apply the following format:
     * <br>
     * <code>Rank | URL | Average Duration |\n</code>
     *
     * @param noDocs Number of entries in the documents relation
     * @param path Output path for the ranks relation
     */
    private static void genRanks(int noDocs, String path) {

        Random rand = new Random(Calendar.getInstance().getTimeInMillis());

        try (BufferedWriter fw = new BufferedWriter(new FileWriter(path))) {
            for (int i = 0; i < noDocs; i++) {
                // Rank
                StringBuilder rank = new StringBuilder(rand.nextInt(100) + "|");
                // URL
                rank.append("url_" + i + "|");
                // Average duration
                rank.append(rand.nextInt(10) + rand.nextInt(50) + "|\n");

                fw.write(rank.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates the files for the visits relation. The visits entries apply the following format:
     * <br>
     * <code>IP Address | URL | Date (YYYY-MM-DD) | Misc. Data (e.g. User-Agent) |\n</code>
     *
     * @param noVisits Number of entries for the visits relation
     * @param noDocs Number of entries in the documents relation
     * @param path Output path for the visits relation
     */
    private static void genVisits(int noVisits, int noDocs, String path) {

        Random rand = new Random(Calendar.getInstance().getTimeInMillis());

        try (BufferedWriter fw = new BufferedWriter(new FileWriter(path))) {
            for (int i = 0; i < noVisits; i++) {

                int year = 2000 + rand.nextInt(10); // yearFilter 3
                int month = rand.nextInt(12) + 1; // month between 1 and 12
                int day = rand.nextInt(27) + 1; // day between 1 and 28

                // IP address
                StringBuilder visit =
                        new StringBuilder(
                                rand.nextInt(256)
                                        + "."
                                        + rand.nextInt(256)
                                        + "."
                                        + rand.nextInt(256)
                                        + "."
                                        + rand.nextInt(256)
                                        + "|");
                // URL
                visit.append("url_" + rand.nextInt(noDocs) + "|");
                // Date (format: YYYY-MM-DD)
                visit.append(year + "-" + month + "-" + day + "|");
                // Miscellaneous data, e.g. User-Agent
                visit.append("0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|\n");

                fw.write(visit.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
