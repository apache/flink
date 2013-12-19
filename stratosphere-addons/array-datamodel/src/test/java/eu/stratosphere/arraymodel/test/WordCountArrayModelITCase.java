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

package eu.stratosphere.arraymodel.test;

import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.arraymodel.example.WordCountArrayTuples;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.TestBase2;

@RunWith(Parameterized.class)
public class WordCountArrayModelITCase extends TestBase2 {

	public static final String TEXT = "Goethe - Faust: Der Tragoedie erster Teil\n" + "Prolog im Himmel.\n"
			+ "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n" + "Erzengel treten vor.\n"
			+ "RAPHAEL: Die Sonne toent, nach alter Weise, In Brudersphaeren Wettgesang,\n"
			+ "Und ihre vorgeschriebne Reise Vollendet sie mit Donnergang. Ihr Anblick\n"
			+ "gibt den Engeln Staerke, Wenn keiner Sie ergruenden mag; die unbegreiflich\n"
			+ "hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "GABRIEL: Und schnell und unbegreiflich schnelle Dreht sich umher der Erde\n"
			+ "Pracht; Es wechselt Paradieseshelle Mit tiefer, schauervoller Nacht. Es\n"
			+ "schaeumt das Meer in breiten Fluessen Am tiefen Grund der Felsen auf, Und\n"
			+ "Fels und Meer wird fortgerissen Im ewig schnellem Sphaerenlauf.\n"
			+ "MICHAEL: Und Stuerme brausen um die Wette Vom Meer aufs Land, vom Land\n"
			+ "aufs Meer, und bilden wuetend eine Kette Der tiefsten Wirkung rings umher.\n"
			+ "Da flammt ein blitzendes Verheeren Dem Pfade vor des Donnerschlags. Doch\n"
			+ "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.\n"
			+ "ZU DREI: Der Anblick gibt den Engeln Staerke, Da keiner dich ergruenden\n"
			+ "mag, Und alle deine hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "MEPHISTOPHELES: Da du, o Herr, dich einmal wieder nahst Und fragst, wie\n"
			+ "alles sich bei uns befinde, Und du mich sonst gewoehnlich gerne sahst, So\n"
			+ "siehst du mich auch unter dem Gesinde. Verzeih, ich kann nicht hohe Worte\n"
			+ "machen, Und wenn mich auch der ganze Kreis verhoehnt; Mein Pathos braechte\n"
			+ "dich gewiss zum Lachen, Haettst du dir nicht das Lachen abgewoehnt. Von\n"
			+ "Sonn' und Welten weiss ich nichts zu sagen, Ich sehe nur, wie sich die\n"
			+ "Menschen plagen. Der kleine Gott der Welt bleibt stets von gleichem\n"
			+ "Schlag, Und ist so wunderlich als wie am ersten Tag. Ein wenig besser\n"
			+ "wuerd er leben, Haettst du ihm nicht den Schein des Himmelslichts gegeben;\n"
			+ "Er nennt's Vernunft und braucht's allein, Nur tierischer als jedes Tier\n"
			+ "zu sein. Er scheint mir, mit Verlaub von euer Gnaden, Wie eine der\n"
			+ "langbeinigen Zikaden, Die immer fliegt und fliegend springt Und gleich im\n"
			+ "Gras ihr altes Liedchen singt; Und laeg er nur noch immer in dem Grase! In\n"
			+ "jeden Quark begraebt er seine Nase.\n"
			+ "DER HERR: Hast du mir weiter nichts zu sagen? Kommst du nur immer\n"
			+ "anzuklagen? Ist auf der Erde ewig dir nichts recht?\n"
			+ "MEPHISTOPHELES: Nein Herr! ich find es dort, wie immer, herzlich\n"
			+ "schlecht. Die Menschen dauern mich in ihren Jammertagen, Ich mag sogar\n"
			+ "die armen selbst nicht plagen.\n" + "DER HERR: Kennst du den Faust?\n" + "MEPHISTOPHELES: Den Doktor?\n"
			+ "DER HERR: Meinen Knecht!\n"
			+ "MEPHISTOPHELES: Fuerwahr! er dient Euch auf besondre Weise. Nicht irdisch\n"
			+ "ist des Toren Trank noch Speise. Ihn treibt die Gaerung in die Ferne, Er\n"
			+ "ist sich seiner Tollheit halb bewusst; Vom Himmel fordert er die schoensten\n"
			+ "Sterne Und von der Erde jede hoechste Lust, Und alle Naeh und alle Ferne\n"
			+ "Befriedigt nicht die tiefbewegte Brust.\n"
			+ "DER HERR: Wenn er mir auch nur verworren dient, So werd ich ihn bald in\n"
			+ "die Klarheit fuehren. Weiss doch der Gaertner, wenn das Baeumchen gruent, Das\n"
			+ "Bluet und Frucht die kuenft'gen Jahre zieren.\n"
			+ "MEPHISTOPHELES: Was wettet Ihr? den sollt Ihr noch verlieren! Wenn Ihr\n"
			+ "mir die Erlaubnis gebt, Ihn meine Strasse sacht zu fuehren.\n"
			+ "DER HERR: Solang er auf der Erde lebt, So lange sei dir's nicht verboten,\n"
			+ "Es irrt der Mensch so lang er strebt.\n"
			+ "MEPHISTOPHELES: Da dank ich Euch; denn mit den Toten Hab ich mich niemals\n"
			+ "gern befangen. Am meisten lieb ich mir die vollen, frischen Wangen. Fuer\n"
			+ "einem Leichnam bin ich nicht zu Haus; Mir geht es wie der Katze mit der Maus.\n"
			+ "DER HERR: Nun gut, es sei dir ueberlassen! Zieh diesen Geist von seinem\n"
			+ "Urquell ab, Und fuehr ihn, kannst du ihn erfassen, Auf deinem Wege mit\n"
			+ "herab, Und steh beschaemt, wenn du bekennen musst: Ein guter Mensch, in\n"
			+ "seinem dunklen Drange, Ist sich des rechten Weges wohl bewusst.\n"
			+ "MEPHISTOPHELES: Schon gut! nur dauert es nicht lange. Mir ist fuer meine\n"
			+ "Wette gar nicht bange. Wenn ich zu meinem Zweck gelange, Erlaubt Ihr mir\n"
			+ "Triumph aus voller Brust. Staub soll er fressen, und mit Lust, Wie meine\n"
			+ "Muhme, die beruehmte Schlange.\n"
			+ "DER HERR: Du darfst auch da nur frei erscheinen; Ich habe deinesgleichen\n"
			+ "nie gehasst. Von allen Geistern, die verneinen, ist mir der Schalk am\n"
			+ "wenigsten zur Last. Des Menschen Taetigkeit kann allzu leicht erschlaffen,\n"
			+ "er liebt sich bald die unbedingte Ruh; Drum geb ich gern ihm den Gesellen\n"
			+ "zu, Der reizt und wirkt und muss als Teufel schaffen. Doch ihr, die echten\n"
			+ "Goettersoehne, Erfreut euch der lebendig reichen Schoene! Das Werdende, das\n"
			+ "ewig wirkt und lebt, Umfass euch mit der Liebe holden Schranken, Und was\n"
			+ "in schwankender Erscheinung schwebt, Befestigt mit dauernden Gedanken!\n"
			+ "(Der Himmel schliesst, die Erzengel verteilen sich.)\n"
			+ "MEPHISTOPHELES (allein): Von Zeit zu Zeit seh ich den Alten gern, Und\n"
			+ "huete mich, mit ihm zu brechen. Es ist gar huebsch von einem grossen Herrn,\n"
			+ "So menschlich mit dem Teufel selbst zu sprechen.";

	public static final String COUNTS = "machen 1\n" + "zeit 2\n" + "heerscharen 1\n" + "keiner 2\n" + "meine 3\n"
			+ "fuehr 1\n" + "triumph 1\n" + "kommst 1\n" + "frei 1\n" + "schaffen 1\n" + "gesinde 1\n"
			+ "langbeinigen 1\n" + "schalk 1\n" + "besser 1\n" + "solang 1\n" + "meer 4\n" + "fragst 1\n"
			+ "gabriel 1\n" + "selbst 2\n" + "bin 1\n" + "sich 7\n" + "du 11\n" + "sogar 1\n" + "geht 1\n"
			+ "immer 4\n" + "mensch 2\n" + "befestigt 1\n" + "lebt 2\n" + "mag 3\n" + "engeln 2\n" + "breiten 1\n"
			+ "blitzendes 1\n" + "tags 1\n" + "sie 2\n" + "plagen 2\n" + "allzu 1\n" + "meisten 1\n" + "o 1\n"
			+ "pfade 1\n" + "kennst 1\n" + "nichts 3\n" + "gedanken 1\n" + "befriedigt 1\n" + "mich 6\n" + "s 3\n"
			+ "es 8\n" + "verneinen 1\n" + "er 13\n" + "gleich 1\n" + "baeumchen 1\n" + "donnergang 1\n"
			+ "wunderlich 1\n" + "reise 1\n" + "urquell 1\n" + "doch 3\n" + "aufs 2\n" + "toten 1\n" + "niemals 1\n"
			+ "eine 2\n" + "hab 1\n" + "darfst 1\n" + "da 5\n" + "gen 1\n" + "einem 2\n" + "teil 1\n" + "das 7\n"
			+ "speise 1\n" + "wenig 1\n" + "sterne 1\n" + "geb 1\n" + "welten 1\n" + "alle 3\n" + "toent 1\n"
			+ "gras 1\n" + "felsen 1\n" + "kette 1\n" + "ich 14\n" + "fuer 2\n" + "als 3\n" + "mein 1\n"
			+ "schoene 1\n" + "verzeih 1\n" + "schwankender 1\n" + "wie 9\n" + "menschlich 1\n" + "gaertner 1\n"
			+ "taetigkeit 1\n" + "bange 1\n" + "liebe 1\n" + "sei 2\n" + "seh 1\n" + "tollheit 1\n" + "am 6\n"
			+ "michael 1\n" + "geist 1\n" + "ab 1\n" + "nahst 1\n" + "vollendet 1\n" + "liebt 1\n" + "brausen 1\n"
			+ "nase 1\n" + "erlaubt 1\n" + "weiss 2\n" + "schnellem 1\n" + "deinem 1\n" + "gleichem 1\n"
			+ "gaerung 1\n" + "dauernden 1\n" + "deines 1\n" + "vorgeschriebne 1\n" + "irdisch 1\n" + "worte 1\n"
			+ "verehren 1\n" + "hohen 2\n" + "weise 2\n" + "kuenft 1\n" + "werdende 1\n" + "wette 2\n" + "wuetend 1\n"
			+ "erscheinung 1\n" + "gar 2\n" + "verlieren 1\n" + "braucht 1\n" + "weiter 1\n" + "trank 1\n"
			+ "tierischer 1\n" + "wohl 1\n" + "verteilen 1\n" + "verhoehnt 1\n" + "schaeumt 1\n" + "himmelslichts 1\n"
			+ "unbedingte 1\n" + "herzlich 1\n" + "anblick 2\n" + "nennt 1\n" + "gruent 1\n" + "bluet 1\n"
			+ "leichnam 1\n" + "erschlaffen 1\n" + "jammertagen 1\n" + "zieh 1\n" + "ihm 3\n" + "besondre 1\n"
			+ "ihn 5\n" + "grossen 1\n" + "vollen 1\n" + "ihr 7\n" + "boten 1\n" + "voller 1\n" + "singt 1\n"
			+ "muhme 1\n" + "schon 1\n" + "last 1\n" + "kleine 1\n" + "paradieseshelle 1\n" + "nein 1\n" + "echten 1\n"
			+ "unter 1\n" + "bei 1\n" + "herr 11\n" + "gern 3\n" + "sphaerenlauf 1\n" + "stets 1\n" + "ganze 1\n"
			+ "braechte 1\n" + "fordert 1\n" + "schoensten 1\n" + "herrlich 2\n" + "gegeben 1\n" + "allein 2\n"
			+ "reichen 1\n" + "schauervoller 1\n" + "musst 1\n" + "recht 1\n" + "bleibt 1\n" + "pracht 1\n"
			+ "treibt 1\n" + "befangen 1\n" + "was 2\n" + "menschen 3\n" + "jede 1\n" + "hohe 1\n" + "tiefsten 1\n"
			+ "bilden 1\n" + "drum 1\n" + "gibt 2\n" + "guter 1\n" + "fuerwahr 1\n" + "im 3\n" + "grund 1\n" + "in 9\n"
			+ "hoechste 1\n" + "schliesst 1\n" + "fels 1\n" + "steh 1\n" + "euer 1\n" + "erster 1\n" + "ersten 3\n"
			+ "goettersoehne 1\n" + "brechen 1\n" + "tiefen 1\n" + "frucht 1\n" + "kreis 1\n" + "siehst 1\n"
			+ "wege 1\n" + "ist 8\n" + "zikaden 1\n" + "frischen 1\n" + "ruh 1\n" + "deine 2\n" + "maus 1\n"
			+ "brudersphaeren 1\n" + "nachher 1\n" + "euch 4\n" + "gnaden 1\n" + "anzuklagen 1\n" + "schlange 1\n"
			+ "staerke 2\n" + "erde 4\n" + "verlaub 1\n" + "sanfte 1\n" + "holden 1\n" + "sonst 1\n" + "treten 1\n"
			+ "sahst 1\n" + "alten 1\n" + "um 1\n" + "wieder 1\n" + "alter 1\n" + "altes 1\n" + "nun 1\n" + "lieb 1\n"
			+ "gesellen 1\n" + "erscheinen 1\n" + "wirkt 2\n" + "haettst 2\n" + "nur 7\n" + "tiefbewegte 1\n"
			+ "lachen 2\n" + "drange 1\n" + "schlag 1\n" + "schein 1\n" + "muss 1\n" + "verworren 1\n" + "weges 1\n"
			+ "allen 1\n" + "gewoehnlich 1\n" + "alles 1\n" + "halb 1\n" + "stuerme 1\n" + "springt 1\n" + "sollt 1\n"
			+ "klarheit 1\n" + "so 6\n" + "erfassen 1\n" + "liedchen 1\n" + "prolog 1\n" + "zur 1\n" + "fressen 1\n"
			+ "zum 1\n" + "faust 2\n" + "erzengel 2\n" + "jahre 1\n" + "sonn 1\n" + "raphael 1\n" + "land 2\n"
			+ "lang 1\n" + "gelange 1\n" + "lust 2\n" + "welt 1\n" + "sehe 1\n" + "ihre 1\n" + "jedes 1\n"
			+ "erfreut 1\n" + "seiner 1\n" + "denn 1\n" + "wandeln 1\n" + "wechselt 1\n" + "jeden 1\n" + "dort 1\n"
			+ "schlecht 1\n" + "wenigsten 1\n" + "wuerd 1\n" + "schranken 1\n" + "bewusst 2\n" + "seinem 2\n"
			+ "gehasst 1\n" + "sein 1\n" + "meinem 1\n" + "meinen 1\n" + "pathos 1\n" + "herrn 1\n" + "lange 2\n"
			+ "herab 1\n" + "diesen 1\n" + "ihren 1\n" + "beruehmte 1\n" + "goethe 1\n" + "tag 3\n" + "tier 1\n"
			+ "quark 1\n" + "dank 1\n" + "seine 1\n" + "teufel 2\n" + "zweck 1\n" + "wenn 7\n" + "soll 1\n"
			+ "wirkung 1\n" + "erlaubnis 1\n" + "lebendig 1\n" + "uns 1\n" + "leicht 1\n" + "gewiss 1\n"
			+ "schnell 1\n" + "und 29\n" + "gerne 1\n" + "rechten 1\n" + "umher 2\n" + "vernunft 1\n" + "grase 1\n"
			+ "nach 1\n" + "leben 1\n" + "gott 1\n" + "der 29\n" + "des 5\n" + "doktor 1\n" + "beschaemt 1\n"
			+ "dreht 1\n" + "habe 1\n" + "sagen 2\n" + "bekennen 1\n" + "dunklen 1\n" + "wettet 1\n" + "den 9\n"
			+ "mephistopheles 9\n" + "dem 4\n" + "auch 4\n" + "kann 2\n" + "armen 1\n" + "mir 9\n" + "strebt 1\n"
			+ "gut 2\n" + "mit 11\n" + "bald 2\n" + "himmlischen 1\n" + "himmel 3\n" + "noch 3\n" + "kannst 1\n"
			+ "deinesgleichen 1\n" + "flammt 1\n" + "ergruenden 2\n" + "nacht 1\n" + "scheint 1\n" + "ferne 2\n"
			+ "tragoedie 1\n" + "abgewoehnt 1\n" + "reizt 1\n" + "geistern 1\n" + "nicht 10\n" + "sacht 1\n"
			+ "unbegreiflich 2\n" + "schnelle 1\n" + "einmal 1\n" + "werd 1\n" + "werke 2\n" + "begraebt 1\n"
			+ "knecht 1\n" + "rings 1\n" + "wird 1\n" + "katze 1\n" + "huete 1\n" + "fortgerissen 1\n" + "gebt 1\n"
			+ "huebsch 1\n" + "hast 1\n" + "irrt 1\n" + "befinde 1\n" + "sind 2\n" + "fuehren 2\n" + "fliegt 1\n"
			+ "ewig 3\n" + "brust 2\n" + "sonne 1\n" + "sprechen 1\n" + "ein 3\n" + "strasse 1\n" + "von 8\n"
			+ "ueberlassen 1\n" + "dir 4\n" + "vom 3\n" + "zu 11\n" + "schwebt 1\n" + "die 22\n" + "vor 2\n"
			+ "wangen 1\n" + "wettgesang 1\n" + "donnerschlags 1\n" + "find 1\n" + "dich 3\n" + "umfass 1\n"
			+ "verboten 1\n" + "laeg 1\n" + "nie 1\n" + "drei 2\n" + "dauern 1\n" + "toren 1\n" + "dauert 1\n"
			+ "verheeren 1\n" + "fliegend 1\n" + "aus 1\n" + "staub 1\n" + "fluessen 1\n" + "haus 1\n" + "auf 5\n"
			+ "dient 2\n" + "tiefer 1\n" + "naeh 1\n" + "zieren 1\n";

	protected String textPath;
	protected String resultPath;
	
	public WordCountArrayModelITCase(Configuration config) {
		super(config);
	}

	@Override
	protected Plan getTestJob() {
		WordCountArrayTuples wc = new WordCountArrayTuples();
		return wc.getPlan(config.getString("WordCountTest#NumSubtasks", "1"), textPath, resultPath);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(COUNTS, resultPath);
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("WordCountTest#NumSubtasks", 4);
		return toParameterList(config);
	}
}
