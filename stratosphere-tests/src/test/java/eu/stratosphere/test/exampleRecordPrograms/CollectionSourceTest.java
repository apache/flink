package eu.stratosphere.test.exampleRecordPrograms;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.CollectionDataSource;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;


/**
 * test the collection and iterator data input using join operator
 *
 */
@RunWith(Parameterized.class)
public class CollectionSourceTest extends TestBase2 {
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

    protected String resultPath;

    public static class Join extends JoinFunction {

        @Override
        public void join(Record value1, Record value2, Collector<Record> out) throws Exception {
            out.collect(new Record(value1.getField(1,StringValue.class),value2.getField(1, IntValue.class)));

        }
    }

    public static class SerializableIteratorTest implements Iterator<List<Object>>,Serializable {

        private static final long serialVersionUID = 1L;
        private String [] s = COUNTS.split("\n");
        private int pos = 0;

        public void remove(){
        }
        public List<Object> next() {
            List<Object> tmp = new ArrayList<Object>();
            tmp.add(pos);
            tmp.add(s[pos++].split(" ")[0]);
            return tmp;
        }
        public boolean hasNext() {
            return pos < s.length;
        }
    }


    public Plan getPlan(String arg1, String arg2) {
        // parse job parameters
        int numSubTasks   = Integer.parseInt(arg1);
        String output    = arg2;


        List<Object> tmp= new ArrayList<Object>();
        int pos = 0;
        for (String s: COUNTS.split("\n")) {
            List<Object> tmpInner= new ArrayList<Object>();
            tmpInner.add(pos++);
            tmpInner.add(Integer.parseInt(s.split(" ")[1]));
            tmp.add(tmpInner);
        }

        //test serializable iterator input, the input record is {id, word}
        CollectionDataSource source = new CollectionDataSource(new SerializableIteratorTest(),"test_iterator");
        //test collection input, the input record is {id, count}
        CollectionDataSource source2 = new CollectionDataSource(tmp, "test_collection");



        JoinOperator join = JoinOperator.builder(Join.class, IntValue.class, 0, 0)
                .input1(source).input2(source2).build();


        FileDataSink out = new FileDataSink(new CsvOutputFormat(), output, join, "Collection Join");
        CsvOutputFormat.configureRecordFormat(out)
                .recordDelimiter('\n')
                .fieldDelimiter(' ')
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);

        Plan plan = new Plan(out, "CollectionDataSource");
        plan.setDefaultParallelism(numSubTasks);
        return plan;
    }


    public CollectionSourceTest(Configuration config) {
        super(config);
    }



    @Override
    protected void preSubmit() throws Exception {
        resultPath = getTempDirPath("result");
    }

    @Override
    protected Plan getTestJob() {
        return getPlan(config.getString("CollectionDataSource#NumSubtasks", "1"),resultPath);
    }

    @Override
    protected void postSubmit() throws Exception {
        // Test results
        compareResultsByLinesInMemory(COUNTS, resultPath);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getConfigurations() {
        Configuration config = new Configuration();
        config.setInteger("CollectionDataSource#NumSubtasks", 4);
        return toParameterList(config);
    }

}
