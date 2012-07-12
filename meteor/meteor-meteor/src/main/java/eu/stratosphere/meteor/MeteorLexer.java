// $ANTLR 3.3 Nov 30, 2010 12:46:29 /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g 2012-07-09 12:58:35
 
package eu.stratosphere.meteor; 


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class MeteorLexer extends Lexer {
    public static final int EOF=-1;
    public static final int T__26=26;
    public static final int T__27=27;
    public static final int T__28=28;
    public static final int T__29=29;
    public static final int T__30=30;
    public static final int T__31=31;
    public static final int T__32=32;
    public static final int T__33=33;
    public static final int T__34=34;
    public static final int T__35=35;
    public static final int T__36=36;
    public static final int T__37=37;
    public static final int T__38=38;
    public static final int T__39=39;
    public static final int T__40=40;
    public static final int T__41=41;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int T__44=44;
    public static final int T__45=45;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int T__50=50;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__59=59;
    public static final int T__60=60;
    public static final int T__61=61;
    public static final int T__62=62;
    public static final int T__63=63;
    public static final int T__64=64;
    public static final int T__65=65;
    public static final int T__66=66;
    public static final int T__67=67;
    public static final int T__68=68;
    public static final int T__69=69;
    public static final int EXPRESSION=4;
    public static final int OPERATOR=5;
    public static final int ID=6;
    public static final int VAR=7;
    public static final int STRING=8;
    public static final int STAR=9;
    public static final int DECIMAL=10;
    public static final int INTEGER=11;
    public static final int UINT=12;
    public static final int LOWER_LETTER=13;
    public static final int UPPER_LETTER=14;
    public static final int DIGIT=15;
    public static final int SIGN=16;
    public static final int COMMENT=17;
    public static final int APOSTROPHE=18;
    public static final int QUOTATION=19;
    public static final int WS=20;
    public static final int UNICODE_ESC=21;
    public static final int OCTAL_ESC=22;
    public static final int ESC_SEQ=23;
    public static final int HEX_DIGIT=24;
    public static final int EXPONENT=25;

    // delegates
    // delegators

    public MeteorLexer() {;} 
    public MeteorLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public MeteorLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "/home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g"; }

    // $ANTLR start "T__26"
    public final void mT__26() throws RecognitionException {
        try {
            int _type = T__26;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:11:7: ( ';' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:11:9: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__26"

    // $ANTLR start "T__27"
    public final void mT__27() throws RecognitionException {
        try {
            int _type = T__27;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:12:7: ( 'using' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:12:9: 'using'
            {
            match("using"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__27"

    // $ANTLR start "T__28"
    public final void mT__28() throws RecognitionException {
        try {
            int _type = T__28;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:13:7: ( '=' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:13:9: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__28"

    // $ANTLR start "T__29"
    public final void mT__29() throws RecognitionException {
        try {
            int _type = T__29;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:14:7: ( 'fn' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:14:9: 'fn'
            {
            match("fn"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__29"

    // $ANTLR start "T__30"
    public final void mT__30() throws RecognitionException {
        try {
            int _type = T__30;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:15:7: ( '(' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:15:9: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__30"

    // $ANTLR start "T__31"
    public final void mT__31() throws RecognitionException {
        try {
            int _type = T__31;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:16:7: ( ',' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:16:9: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__31"

    // $ANTLR start "T__32"
    public final void mT__32() throws RecognitionException {
        try {
            int _type = T__32;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:17:7: ( ')' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:17:9: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__32"

    // $ANTLR start "T__33"
    public final void mT__33() throws RecognitionException {
        try {
            int _type = T__33;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:18:7: ( 'javaudf' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:18:9: 'javaudf'
            {
            match("javaudf"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__33"

    // $ANTLR start "T__34"
    public final void mT__34() throws RecognitionException {
        try {
            int _type = T__34;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:19:7: ( '?' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:19:9: '?'
            {
            match('?'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__34"

    // $ANTLR start "T__35"
    public final void mT__35() throws RecognitionException {
        try {
            int _type = T__35;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:20:7: ( ':' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:20:9: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__35"

    // $ANTLR start "T__36"
    public final void mT__36() throws RecognitionException {
        try {
            int _type = T__36;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:21:7: ( 'if' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:21:9: 'if'
            {
            match("if"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__36"

    // $ANTLR start "T__37"
    public final void mT__37() throws RecognitionException {
        try {
            int _type = T__37;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:22:7: ( 'or' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:22:9: 'or'
            {
            match("or"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__37"

    // $ANTLR start "T__38"
    public final void mT__38() throws RecognitionException {
        try {
            int _type = T__38;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:23:7: ( '||' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:23:9: '||'
            {
            match("||"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__38"

    // $ANTLR start "T__39"
    public final void mT__39() throws RecognitionException {
        try {
            int _type = T__39;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:24:7: ( 'and' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:24:9: 'and'
            {
            match("and"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__39"

    // $ANTLR start "T__40"
    public final void mT__40() throws RecognitionException {
        try {
            int _type = T__40;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:25:7: ( '&&' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:25:9: '&&'
            {
            match("&&"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__40"

    // $ANTLR start "T__41"
    public final void mT__41() throws RecognitionException {
        try {
            int _type = T__41;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:26:7: ( 'not' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:26:9: 'not'
            {
            match("not"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__41"

    // $ANTLR start "T__42"
    public final void mT__42() throws RecognitionException {
        try {
            int _type = T__42;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:27:7: ( 'in' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:27:9: 'in'
            {
            match("in"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__42"

    // $ANTLR start "T__43"
    public final void mT__43() throws RecognitionException {
        try {
            int _type = T__43;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:28:7: ( '<=' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:28:9: '<='
            {
            match("<="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__43"

    // $ANTLR start "T__44"
    public final void mT__44() throws RecognitionException {
        try {
            int _type = T__44;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:29:7: ( '>=' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:29:9: '>='
            {
            match(">="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__44"

    // $ANTLR start "T__45"
    public final void mT__45() throws RecognitionException {
        try {
            int _type = T__45;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:30:7: ( '<' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:30:9: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__45"

    // $ANTLR start "T__46"
    public final void mT__46() throws RecognitionException {
        try {
            int _type = T__46;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:31:7: ( '>' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:31:9: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__46"

    // $ANTLR start "T__47"
    public final void mT__47() throws RecognitionException {
        try {
            int _type = T__47;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:32:7: ( '==' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:32:9: '=='
            {
            match("=="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__47"

    // $ANTLR start "T__48"
    public final void mT__48() throws RecognitionException {
        try {
            int _type = T__48;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:33:7: ( '!=' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:33:9: '!='
            {
            match("!="); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__48"

    // $ANTLR start "T__49"
    public final void mT__49() throws RecognitionException {
        try {
            int _type = T__49;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:34:7: ( '+' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:34:9: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__49"

    // $ANTLR start "T__50"
    public final void mT__50() throws RecognitionException {
        try {
            int _type = T__50;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:35:7: ( '-' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:35:9: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__50"

    // $ANTLR start "T__51"
    public final void mT__51() throws RecognitionException {
        try {
            int _type = T__51;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:36:7: ( '/' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:36:9: '/'
            {
            match('/'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__51"

    // $ANTLR start "T__52"
    public final void mT__52() throws RecognitionException {
        try {
            int _type = T__52;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:37:7: ( '++' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:37:9: '++'
            {
            match("++"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__52"

    // $ANTLR start "T__53"
    public final void mT__53() throws RecognitionException {
        try {
            int _type = T__53;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:38:7: ( '--' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:38:9: '--'
            {
            match("--"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__53"

    // $ANTLR start "T__54"
    public final void mT__54() throws RecognitionException {
        try {
            int _type = T__54;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:39:7: ( '!' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:39:9: '!'
            {
            match('!'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__54"

    // $ANTLR start "T__55"
    public final void mT__55() throws RecognitionException {
        try {
            int _type = T__55;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:40:7: ( '~' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:40:9: '~'
            {
            match('~'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__55"

    // $ANTLR start "T__56"
    public final void mT__56() throws RecognitionException {
        try {
            int _type = T__56;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:41:7: ( 'as' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:41:9: 'as'
            {
            match("as"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__56"

    // $ANTLR start "T__57"
    public final void mT__57() throws RecognitionException {
        try {
            int _type = T__57;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:42:7: ( '.' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:42:9: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__57"

    // $ANTLR start "T__58"
    public final void mT__58() throws RecognitionException {
        try {
            int _type = T__58;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:43:7: ( '{' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:43:9: '{'
            {
            match('{'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__58"

    // $ANTLR start "T__59"
    public final void mT__59() throws RecognitionException {
        try {
            int _type = T__59;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:44:7: ( '}' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:44:9: '}'
            {
            match('}'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__59"

    // $ANTLR start "T__60"
    public final void mT__60() throws RecognitionException {
        try {
            int _type = T__60;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:45:7: ( 'true' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:45:9: 'true'
            {
            match("true"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__60"

    // $ANTLR start "T__61"
    public final void mT__61() throws RecognitionException {
        try {
            int _type = T__61;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:46:7: ( 'false' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:46:9: 'false'
            {
            match("false"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__61"

    // $ANTLR start "T__62"
    public final void mT__62() throws RecognitionException {
        try {
            int _type = T__62;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:47:7: ( 'null' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:47:9: 'null'
            {
            match("null"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__62"

    // $ANTLR start "T__63"
    public final void mT__63() throws RecognitionException {
        try {
            int _type = T__63;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:48:7: ( '[' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:48:9: '['
            {
            match('['); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__63"

    // $ANTLR start "T__64"
    public final void mT__64() throws RecognitionException {
        try {
            int _type = T__64;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:49:7: ( ']' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:49:9: ']'
            {
            match(']'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__64"

    // $ANTLR start "T__65"
    public final void mT__65() throws RecognitionException {
        try {
            int _type = T__65;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:50:7: ( 'read' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:50:9: 'read'
            {
            match("read"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__65"

    // $ANTLR start "T__66"
    public final void mT__66() throws RecognitionException {
        try {
            int _type = T__66;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:51:7: ( 'from' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:51:9: 'from'
            {
            match("from"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__66"

    // $ANTLR start "T__67"
    public final void mT__67() throws RecognitionException {
        try {
            int _type = T__67;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:52:7: ( 'write' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:52:9: 'write'
            {
            match("write"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__67"

    // $ANTLR start "T__68"
    public final void mT__68() throws RecognitionException {
        try {
            int _type = T__68;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:53:7: ( 'to' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:53:9: 'to'
            {
            match("to"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__68"

    // $ANTLR start "T__69"
    public final void mT__69() throws RecognitionException {
        try {
            int _type = T__69;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:54:7: ( 'preserve' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:54:9: 'preserve'
            {
            match("preserve"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__69"

    // $ANTLR start "LOWER_LETTER"
    public final void mLOWER_LETTER() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:349:2: ( 'a' .. 'z' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:349:4: 'a' .. 'z'
            {
            matchRange('a','z'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "LOWER_LETTER"

    // $ANTLR start "UPPER_LETTER"
    public final void mUPPER_LETTER() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:352:2: ( 'A' .. 'Z' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:352:4: 'A' .. 'Z'
            {
            matchRange('A','Z'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "UPPER_LETTER"

    // $ANTLR start "DIGIT"
    public final void mDIGIT() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:355:2: ( '0' .. '9' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:355:4: '0' .. '9'
            {
            matchRange('0','9'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "DIGIT"

    // $ANTLR start "SIGN"
    public final void mSIGN() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:357:14: ( ( '+' | '-' ) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:357:16: ( '+' | '-' )
            {
            if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "SIGN"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            int _type = ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:359:4: ( ( LOWER_LETTER | UPPER_LETTER ) ( LOWER_LETTER | UPPER_LETTER | DIGIT | '_' )* )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:359:6: ( LOWER_LETTER | UPPER_LETTER ) ( LOWER_LETTER | UPPER_LETTER | DIGIT | '_' )*
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:359:36: ( LOWER_LETTER | UPPER_LETTER | DIGIT | '_' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')||(LA1_0>='A' && LA1_0<='Z')||LA1_0=='_'||(LA1_0>='a' && LA1_0<='z')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            	    {
            	    if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ID"

    // $ANTLR start "VAR"
    public final void mVAR() throws RecognitionException {
        try {
            int _type = VAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:361:5: ( '$' ID )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:361:7: '$' ID
            {
            match('$'); 
            mID(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "VAR"

    // $ANTLR start "STAR"
    public final void mSTAR() throws RecognitionException {
        try {
            int _type = STAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:363:6: ( '*' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:363:8: '*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "STAR"

    // $ANTLR start "COMMENT"
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:366:5: ( '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' | '/*' ( options {greedy=false; } : . )* '*/' )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0=='/') ) {
                int LA5_1 = input.LA(2);

                if ( (LA5_1=='/') ) {
                    alt5=1;
                }
                else if ( (LA5_1=='*') ) {
                    alt5=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 5, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:366:9: '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
                    {
                    match("//"); 

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:366:14: (~ ( '\\n' | '\\r' ) )*
                    loop2:
                    do {
                        int alt2=2;
                        int LA2_0 = input.LA(1);

                        if ( ((LA2_0>='\u0000' && LA2_0<='\t')||(LA2_0>='\u000B' && LA2_0<='\f')||(LA2_0>='\u000E' && LA2_0<='\uFFFF')) ) {
                            alt2=1;
                        }


                        switch (alt2) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:366:14: ~ ( '\\n' | '\\r' )
                    	    {
                    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='\t')||(input.LA(1)>='\u000B' && input.LA(1)<='\f')||(input.LA(1)>='\u000E' && input.LA(1)<='\uFFFF') ) {
                    	        input.consume();

                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;}


                    	    }
                    	    break;

                    	default :
                    	    break loop2;
                        }
                    } while (true);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:366:28: ( '\\r' )?
                    int alt3=2;
                    int LA3_0 = input.LA(1);

                    if ( (LA3_0=='\r') ) {
                        alt3=1;
                    }
                    switch (alt3) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:366:28: '\\r'
                            {
                            match('\r'); 

                            }
                            break;

                    }

                    match('\n'); 
                    _channel=HIDDEN;

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:367:9: '/*' ( options {greedy=false; } : . )* '*/'
                    {
                    match("/*"); 

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:367:14: ( options {greedy=false; } : . )*
                    loop4:
                    do {
                        int alt4=2;
                        int LA4_0 = input.LA(1);

                        if ( (LA4_0=='*') ) {
                            int LA4_1 = input.LA(2);

                            if ( (LA4_1=='/') ) {
                                alt4=2;
                            }
                            else if ( ((LA4_1>='\u0000' && LA4_1<='.')||(LA4_1>='0' && LA4_1<='\uFFFF')) ) {
                                alt4=1;
                            }


                        }
                        else if ( ((LA4_0>='\u0000' && LA4_0<=')')||(LA4_0>='+' && LA4_0<='\uFFFF')) ) {
                            alt4=1;
                        }


                        switch (alt4) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:367:42: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop4;
                        }
                    } while (true);

                    match("*/"); 

                    _channel=HIDDEN;

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COMMENT"

    // $ANTLR start "APOSTROPHE"
    public final void mAPOSTROPHE() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:371:3: ( '\\'' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:371:5: '\\''
            {
            match('\''); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "APOSTROPHE"

    // $ANTLR start "QUOTATION"
    public final void mQUOTATION() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:374:3: ( '\\\"' )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:374:5: '\\\"'
            {
            match('\"'); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "QUOTATION"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:376:5: ( ( ' ' | '\\t' | '\\n' | '\\r' )+ )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:376:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:376:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            int cnt6=0;
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0>='\t' && LA6_0<='\n')||LA6_0=='\r'||LA6_0==' ') ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
            	    {
            	    if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    if ( cnt6 >= 1 ) break loop6;
                        EarlyExitException eee =
                            new EarlyExitException(6, input);
                        throw eee;
                }
                cnt6++;
            } while (true);

             skip(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "STRING"
    public final void mSTRING() throws RecognitionException {
        try {
            int _type = STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:2: ( ( QUOTATION ( options {greedy=false; } : . )* QUOTATION | APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE ) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:4: ( QUOTATION ( options {greedy=false; } : . )* QUOTATION | APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE )
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:4: ( QUOTATION ( options {greedy=false; } : . )* QUOTATION | APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE )
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0=='\"') ) {
                alt9=1;
            }
            else if ( (LA9_0=='\'') ) {
                alt9=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 9, 0, input);

                throw nvae;
            }
            switch (alt9) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:5: QUOTATION ( options {greedy=false; } : . )* QUOTATION
                    {
                    mQUOTATION(); 
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:15: ( options {greedy=false; } : . )*
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( (LA7_0=='\"') ) {
                            alt7=2;
                        }
                        else if ( ((LA7_0>='\u0000' && LA7_0<='!')||(LA7_0>='#' && LA7_0<='\uFFFF')) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:42: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop7;
                        }
                    } while (true);

                    mQUOTATION(); 

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:58: APOSTROPHE ( options {greedy=false; } : . )* APOSTROPHE
                    {
                    mAPOSTROPHE(); 
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:69: ( options {greedy=false; } : . )*
                    loop8:
                    do {
                        int alt8=2;
                        int LA8_0 = input.LA(1);

                        if ( (LA8_0=='\'') ) {
                            alt8=2;
                        }
                        else if ( ((LA8_0>='\u0000' && LA8_0<='&')||(LA8_0>='(' && LA8_0<='\uFFFF')) ) {
                            alt8=1;
                        }


                        switch (alt8) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:379:96: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop8;
                        }
                    } while (true);

                    mAPOSTROPHE(); 

                    }
                    break;

            }

             setText(getText().substring(1, getText().length()-1)); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "STRING"

    // $ANTLR start "ESC_SEQ"
    public final void mESC_SEQ() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:384:5: ( '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' ) | UNICODE_ESC | OCTAL_ESC )
            int alt10=3;
            int LA10_0 = input.LA(1);

            if ( (LA10_0=='\\') ) {
                switch ( input.LA(2) ) {
                case '\"':
                case '\'':
                case '\\':
                case 'b':
                case 'f':
                case 'n':
                case 'r':
                case 't':
                    {
                    alt10=1;
                    }
                    break;
                case 'u':
                    {
                    alt10=2;
                    }
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                    {
                    alt10=3;
                    }
                    break;
                default:
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 1, input);

                    throw nvae;
                }

            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;
            }
            switch (alt10) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:384:9: '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' )
                    {
                    match('\\'); 
                    if ( input.LA(1)=='\"'||input.LA(1)=='\''||input.LA(1)=='\\'||input.LA(1)=='b'||input.LA(1)=='f'||input.LA(1)=='n'||input.LA(1)=='r'||input.LA(1)=='t' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:385:9: UNICODE_ESC
                    {
                    mUNICODE_ESC(); 

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:386:9: OCTAL_ESC
                    {
                    mOCTAL_ESC(); 

                    }
                    break;

            }
        }
        finally {
        }
    }
    // $ANTLR end "ESC_SEQ"

    // $ANTLR start "OCTAL_ESC"
    public final void mOCTAL_ESC() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:5: ( '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) )
            int alt11=3;
            int LA11_0 = input.LA(1);

            if ( (LA11_0=='\\') ) {
                int LA11_1 = input.LA(2);

                if ( ((LA11_1>='0' && LA11_1<='3')) ) {
                    int LA11_2 = input.LA(3);

                    if ( ((LA11_2>='0' && LA11_2<='7')) ) {
                        int LA11_4 = input.LA(4);

                        if ( ((LA11_4>='0' && LA11_4<='7')) ) {
                            alt11=1;
                        }
                        else {
                            alt11=2;}
                    }
                    else {
                        alt11=3;}
                }
                else if ( ((LA11_1>='4' && LA11_1<='7')) ) {
                    int LA11_3 = input.LA(3);

                    if ( ((LA11_3>='0' && LA11_3<='7')) ) {
                        alt11=2;
                    }
                    else {
                        alt11=3;}
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 11, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }
            switch (alt11) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:9: '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:14: ( '0' .. '3' )
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:15: '0' .. '3'
                    {
                    matchRange('0','3'); 

                    }

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:25: ( '0' .. '7' )
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:36: ( '0' .. '7' )
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:391:37: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:392:9: '\\\\' ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:392:14: ( '0' .. '7' )
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:392:15: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:392:25: ( '0' .. '7' )
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:392:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:393:9: '\\\\' ( '0' .. '7' )
                    {
                    match('\\'); 
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:393:14: ( '0' .. '7' )
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:393:15: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;

            }
        }
        finally {
        }
    }
    // $ANTLR end "OCTAL_ESC"

    // $ANTLR start "UNICODE_ESC"
    public final void mUNICODE_ESC() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:397:13: ( '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:397:17: '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
            {
            match('\\'); 
            match('u'); 
            mHEX_DIGIT(); 
            mHEX_DIGIT(); 
            mHEX_DIGIT(); 
            mHEX_DIGIT(); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "UNICODE_ESC"

    // $ANTLR start "HEX_DIGIT"
    public final void mHEX_DIGIT() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:400:11: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:400:13: ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' )
            {
            if ( (input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='F')||(input.LA(1)>='a' && input.LA(1)<='f') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}


            }

        }
        finally {
        }
    }
    // $ANTLR end "HEX_DIGIT"

    // $ANTLR start "UINT"
    public final void mUINT() throws RecognitionException {
        try {
            int _type = UINT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:403:6: ( ( '0' .. '9' )+ )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:403:8: ( '0' .. '9' )+
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:403:8: ( '0' .. '9' )+
            int cnt12=0;
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( ((LA12_0>='0' && LA12_0<='9')) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:403:8: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt12 >= 1 ) break loop12;
                        EarlyExitException eee =
                            new EarlyExitException(12, input);
                        throw eee;
                }
                cnt12++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "UINT"

    // $ANTLR start "INTEGER"
    public final void mINTEGER() throws RecognitionException {
        try {
            int _type = INTEGER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:9: ( ( '+' | '-' )? UINT )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:11: ( '+' | '-' )? UINT
            {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:11: ( '+' | '-' )?
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0=='+'||LA13_0=='-') ) {
                alt13=1;
            }
            switch (alt13) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}


                    }
                    break;

            }

            mUINT(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "INTEGER"

    // $ANTLR start "DECIMAL"
    public final void mDECIMAL() throws RecognitionException {
        try {
            int _type = DECIMAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:5: ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT )
            int alt20=3;
            alt20 = dfa20.predict(input);
            switch (alt20) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:9: ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )?
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:9: ( '0' .. '9' )+
                    int cnt14=0;
                    loop14:
                    do {
                        int alt14=2;
                        int LA14_0 = input.LA(1);

                        if ( ((LA14_0>='0' && LA14_0<='9')) ) {
                            alt14=1;
                        }


                        switch (alt14) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:10: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt14 >= 1 ) break loop14;
                                EarlyExitException eee =
                                    new EarlyExitException(14, input);
                                throw eee;
                        }
                        cnt14++;
                    } while (true);

                    match('.'); 
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:25: ( '0' .. '9' )*
                    loop15:
                    do {
                        int alt15=2;
                        int LA15_0 = input.LA(1);

                        if ( ((LA15_0>='0' && LA15_0<='9')) ) {
                            alt15=1;
                        }


                        switch (alt15) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:26: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    break loop15;
                        }
                    } while (true);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:37: ( EXPONENT )?
                    int alt16=2;
                    int LA16_0 = input.LA(1);

                    if ( (LA16_0=='E'||LA16_0=='e') ) {
                        alt16=1;
                    }
                    switch (alt16) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:408:37: EXPONENT
                            {
                            mEXPONENT(); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:409:9: '.' ( '0' .. '9' )+ ( EXPONENT )?
                    {
                    match('.'); 
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:409:13: ( '0' .. '9' )+
                    int cnt17=0;
                    loop17:
                    do {
                        int alt17=2;
                        int LA17_0 = input.LA(1);

                        if ( ((LA17_0>='0' && LA17_0<='9')) ) {
                            alt17=1;
                        }


                        switch (alt17) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:409:14: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt17 >= 1 ) break loop17;
                                EarlyExitException eee =
                                    new EarlyExitException(17, input);
                                throw eee;
                        }
                        cnt17++;
                    } while (true);

                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:409:25: ( EXPONENT )?
                    int alt18=2;
                    int LA18_0 = input.LA(1);

                    if ( (LA18_0=='E'||LA18_0=='e') ) {
                        alt18=1;
                    }
                    switch (alt18) {
                        case 1 :
                            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:409:25: EXPONENT
                            {
                            mEXPONENT(); 

                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:410:9: ( '0' .. '9' )+ EXPONENT
                    {
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:410:9: ( '0' .. '9' )+
                    int cnt19=0;
                    loop19:
                    do {
                        int alt19=2;
                        int LA19_0 = input.LA(1);

                        if ( ((LA19_0>='0' && LA19_0<='9')) ) {
                            alt19=1;
                        }


                        switch (alt19) {
                    	case 1 :
                    	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:410:10: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt19 >= 1 ) break loop19;
                                EarlyExitException eee =
                                    new EarlyExitException(19, input);
                                throw eee;
                        }
                        cnt19++;
                    } while (true);

                    mEXPONENT(); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "DECIMAL"

    // $ANTLR start "EXPONENT"
    public final void mEXPONENT() throws RecognitionException {
        try {
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:413:10: ( ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ )
            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:413:12: ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:413:22: ( '+' | '-' )?
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( (LA21_0=='+'||LA21_0=='-') ) {
                alt21=1;
            }
            switch (alt21) {
                case 1 :
                    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}


                    }
                    break;

            }

            // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:413:33: ( '0' .. '9' )+
            int cnt22=0;
            loop22:
            do {
                int alt22=2;
                int LA22_0 = input.LA(1);

                if ( ((LA22_0>='0' && LA22_0<='9')) ) {
                    alt22=1;
                }


                switch (alt22) {
            	case 1 :
            	    // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:413:34: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt22 >= 1 ) break loop22;
                        EarlyExitException eee =
                            new EarlyExitException(22, input);
                        throw eee;
                }
                cnt22++;
            } while (true);


            }

        }
        finally {
        }
    }
    // $ANTLR end "EXPONENT"

    public void mTokens() throws RecognitionException {
        // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:8: ( T__26 | T__27 | T__28 | T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | T__36 | T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | T__58 | T__59 | T__60 | T__61 | T__62 | T__63 | T__64 | T__65 | T__66 | T__67 | T__68 | T__69 | ID | VAR | STAR | COMMENT | WS | STRING | UINT | INTEGER | DECIMAL )
        int alt23=53;
        alt23 = dfa23.predict(input);
        switch (alt23) {
            case 1 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:10: T__26
                {
                mT__26(); 

                }
                break;
            case 2 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:16: T__27
                {
                mT__27(); 

                }
                break;
            case 3 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:22: T__28
                {
                mT__28(); 

                }
                break;
            case 4 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:28: T__29
                {
                mT__29(); 

                }
                break;
            case 5 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:34: T__30
                {
                mT__30(); 

                }
                break;
            case 6 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:40: T__31
                {
                mT__31(); 

                }
                break;
            case 7 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:46: T__32
                {
                mT__32(); 

                }
                break;
            case 8 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:52: T__33
                {
                mT__33(); 

                }
                break;
            case 9 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:58: T__34
                {
                mT__34(); 

                }
                break;
            case 10 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:64: T__35
                {
                mT__35(); 

                }
                break;
            case 11 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:70: T__36
                {
                mT__36(); 

                }
                break;
            case 12 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:76: T__37
                {
                mT__37(); 

                }
                break;
            case 13 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:82: T__38
                {
                mT__38(); 

                }
                break;
            case 14 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:88: T__39
                {
                mT__39(); 

                }
                break;
            case 15 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:94: T__40
                {
                mT__40(); 

                }
                break;
            case 16 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:100: T__41
                {
                mT__41(); 

                }
                break;
            case 17 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:106: T__42
                {
                mT__42(); 

                }
                break;
            case 18 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:112: T__43
                {
                mT__43(); 

                }
                break;
            case 19 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:118: T__44
                {
                mT__44(); 

                }
                break;
            case 20 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:124: T__45
                {
                mT__45(); 

                }
                break;
            case 21 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:130: T__46
                {
                mT__46(); 

                }
                break;
            case 22 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:136: T__47
                {
                mT__47(); 

                }
                break;
            case 23 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:142: T__48
                {
                mT__48(); 

                }
                break;
            case 24 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:148: T__49
                {
                mT__49(); 

                }
                break;
            case 25 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:154: T__50
                {
                mT__50(); 

                }
                break;
            case 26 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:160: T__51
                {
                mT__51(); 

                }
                break;
            case 27 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:166: T__52
                {
                mT__52(); 

                }
                break;
            case 28 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:172: T__53
                {
                mT__53(); 

                }
                break;
            case 29 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:178: T__54
                {
                mT__54(); 

                }
                break;
            case 30 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:184: T__55
                {
                mT__55(); 

                }
                break;
            case 31 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:190: T__56
                {
                mT__56(); 

                }
                break;
            case 32 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:196: T__57
                {
                mT__57(); 

                }
                break;
            case 33 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:202: T__58
                {
                mT__58(); 

                }
                break;
            case 34 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:208: T__59
                {
                mT__59(); 

                }
                break;
            case 35 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:214: T__60
                {
                mT__60(); 

                }
                break;
            case 36 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:220: T__61
                {
                mT__61(); 

                }
                break;
            case 37 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:226: T__62
                {
                mT__62(); 

                }
                break;
            case 38 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:232: T__63
                {
                mT__63(); 

                }
                break;
            case 39 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:238: T__64
                {
                mT__64(); 

                }
                break;
            case 40 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:244: T__65
                {
                mT__65(); 

                }
                break;
            case 41 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:250: T__66
                {
                mT__66(); 

                }
                break;
            case 42 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:256: T__67
                {
                mT__67(); 

                }
                break;
            case 43 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:262: T__68
                {
                mT__68(); 

                }
                break;
            case 44 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:268: T__69
                {
                mT__69(); 

                }
                break;
            case 45 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:274: ID
                {
                mID(); 

                }
                break;
            case 46 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:277: VAR
                {
                mVAR(); 

                }
                break;
            case 47 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:281: STAR
                {
                mSTAR(); 

                }
                break;
            case 48 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:286: COMMENT
                {
                mCOMMENT(); 

                }
                break;
            case 49 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:294: WS
                {
                mWS(); 

                }
                break;
            case 50 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:297: STRING
                {
                mSTRING(); 

                }
                break;
            case 51 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:304: UINT
                {
                mUINT(); 

                }
                break;
            case 52 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:309: INTEGER
                {
                mINTEGER(); 

                }
                break;
            case 53 :
                // /home/arv/workspace/private/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:1:317: DECIMAL
                {
                mDECIMAL(); 

                }
                break;

        }

    }


    protected DFA20 dfa20 = new DFA20(this);
    protected DFA23 dfa23 = new DFA23(this);
    static final String DFA20_eotS =
        "\5\uffff";
    static final String DFA20_eofS =
        "\5\uffff";
    static final String DFA20_minS =
        "\2\56\3\uffff";
    static final String DFA20_maxS =
        "\1\71\1\145\3\uffff";
    static final String DFA20_acceptS =
        "\2\uffff\1\2\1\1\1\3";
    static final String DFA20_specialS =
        "\5\uffff}>";
    static final String[] DFA20_transitionS = {
            "\1\2\1\uffff\12\1",
            "\1\3\1\uffff\12\1\13\uffff\1\4\37\uffff\1\4",
            "",
            "",
            ""
    };

    static final short[] DFA20_eot = DFA.unpackEncodedString(DFA20_eotS);
    static final short[] DFA20_eof = DFA.unpackEncodedString(DFA20_eofS);
    static final char[] DFA20_min = DFA.unpackEncodedStringToUnsignedChars(DFA20_minS);
    static final char[] DFA20_max = DFA.unpackEncodedStringToUnsignedChars(DFA20_maxS);
    static final short[] DFA20_accept = DFA.unpackEncodedString(DFA20_acceptS);
    static final short[] DFA20_special = DFA.unpackEncodedString(DFA20_specialS);
    static final short[][] DFA20_transition;

    static {
        int numStates = DFA20_transitionS.length;
        DFA20_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA20_transition[i] = DFA.unpackEncodedString(DFA20_transitionS[i]);
        }
    }

    class DFA20 extends DFA {

        public DFA20(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 20;
            this.eot = DFA20_eot;
            this.eof = DFA20_eof;
            this.min = DFA20_min;
            this.max = DFA20_max;
            this.accept = DFA20_accept;
            this.special = DFA20_special;
            this.transition = DFA20_transition;
        }
        public String getDescription() {
            return "407:1: DECIMAL : ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT );";
        }
    }
    static final String DFA23_eotS =
        "\2\uffff\1\41\1\51\1\41\3\uffff\1\41\2\uffff\2\41\1\uffff\1\41\1"+
        "\uffff\1\41\1\66\1\70\1\72\1\74\1\77\1\101\1\uffff\1\102\2\uffff"+
        "\1\41\2\uffff\3\41\5\uffff\1\111\1\41\2\uffff\1\113\3\41\1\117\1"+
        "\120\1\121\1\41\1\123\2\41\17\uffff\1\41\1\127\3\41\1\uffff\1\41"+
        "\1\uffff\3\41\3\uffff\1\137\1\uffff\1\140\2\41\1\uffff\5\41\1\150"+
        "\1\41\2\uffff\1\152\1\153\1\154\2\41\1\157\1\160\1\uffff\1\41\3"+
        "\uffff\1\162\1\41\2\uffff\1\41\1\uffff\1\41\1\166\1\41\1\uffff\1"+
        "\170\1\uffff";
    static final String DFA23_eofS =
        "\171\uffff";
    static final String DFA23_minS =
        "\1\11\1\uffff\1\163\1\75\1\141\3\uffff\1\141\2\uffff\1\146\1\162"+
        "\1\uffff\1\156\1\uffff\1\157\3\75\1\53\1\55\1\52\1\uffff\1\60\2"+
        "\uffff\1\157\2\uffff\1\145\2\162\5\uffff\1\56\1\151\2\uffff\1\60"+
        "\1\154\1\157\1\166\3\60\1\144\1\60\1\164\1\154\17\uffff\1\165\1"+
        "\60\1\141\1\151\1\145\1\uffff\1\156\1\uffff\1\163\1\155\1\141\3"+
        "\uffff\1\60\1\uffff\1\60\1\154\1\145\1\uffff\1\144\1\164\1\163\1"+
        "\147\1\145\1\60\1\165\2\uffff\3\60\2\145\2\60\1\uffff\1\144\3\uffff"+
        "\1\60\1\162\2\uffff\1\146\1\uffff\1\166\1\60\1\145\1\uffff\1\60"+
        "\1\uffff";
    static final String DFA23_maxS =
        "\1\176\1\uffff\1\163\1\75\1\162\3\uffff\1\141\2\uffff\1\156\1\162"+
        "\1\uffff\1\163\1\uffff\1\165\3\75\2\71\1\57\1\uffff\1\71\2\uffff"+
        "\1\162\2\uffff\1\145\2\162\5\uffff\1\145\1\151\2\uffff\1\172\1\154"+
        "\1\157\1\166\3\172\1\144\1\172\1\164\1\154\17\uffff\1\165\1\172"+
        "\1\141\1\151\1\145\1\uffff\1\156\1\uffff\1\163\1\155\1\141\3\uffff"+
        "\1\172\1\uffff\1\172\1\154\1\145\1\uffff\1\144\1\164\1\163\1\147"+
        "\1\145\1\172\1\165\2\uffff\3\172\2\145\2\172\1\uffff\1\144\3\uffff"+
        "\1\172\1\162\2\uffff\1\146\1\uffff\1\166\1\172\1\145\1\uffff\1\172"+
        "\1\uffff";
    static final String DFA23_acceptS =
        "\1\uffff\1\1\3\uffff\1\5\1\6\1\7\1\uffff\1\11\1\12\2\uffff\1\15"+
        "\1\uffff\1\17\7\uffff\1\36\1\uffff\1\41\1\42\1\uffff\1\46\1\47\3"+
        "\uffff\1\55\1\56\1\57\1\61\1\62\2\uffff\1\26\1\3\13\uffff\1\22\1"+
        "\24\1\23\1\25\1\27\1\35\1\33\1\30\1\64\1\34\1\31\1\60\1\32\1\40"+
        "\1\65\5\uffff\1\63\1\uffff\1\4\3\uffff\1\13\1\21\1\14\1\uffff\1"+
        "\37\3\uffff\1\53\7\uffff\1\16\1\20\7\uffff\1\51\1\uffff\1\45\1\43"+
        "\1\50\2\uffff\1\2\1\44\1\uffff\1\52\3\uffff\1\10\1\uffff\1\54";
    static final String DFA23_specialS =
        "\171\uffff}>";
    static final String[] DFA23_transitionS = {
            "\2\44\2\uffff\1\44\22\uffff\1\44\1\23\1\45\1\uffff\1\42\1\uffff"+
            "\1\17\1\45\1\5\1\7\1\43\1\24\1\6\1\25\1\30\1\26\12\46\1\12\1"+
            "\1\1\21\1\3\1\22\1\11\1\uffff\32\41\1\34\1\uffff\1\35\3\uffff"+
            "\1\16\4\41\1\4\2\41\1\13\1\10\3\41\1\20\1\14\1\40\1\41\1\36"+
            "\1\41\1\33\1\2\1\41\1\37\3\41\1\31\1\15\1\32\1\27",
            "",
            "\1\47",
            "\1\50",
            "\1\53\14\uffff\1\52\3\uffff\1\54",
            "",
            "",
            "",
            "\1\55",
            "",
            "",
            "\1\56\7\uffff\1\57",
            "\1\60",
            "",
            "\1\61\4\uffff\1\62",
            "",
            "\1\63\5\uffff\1\64",
            "\1\65",
            "\1\67",
            "\1\71",
            "\1\73\4\uffff\12\75",
            "\1\76\2\uffff\12\75",
            "\1\100\4\uffff\1\100",
            "",
            "\12\103",
            "",
            "",
            "\1\105\2\uffff\1\104",
            "",
            "",
            "\1\106",
            "\1\107",
            "\1\110",
            "",
            "",
            "",
            "",
            "",
            "\1\103\1\uffff\12\46\13\uffff\1\103\37\uffff\1\103",
            "\1\112",
            "",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\114",
            "\1\115",
            "\1\116",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\122",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\124",
            "\1\125",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\126",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\130",
            "\1\131",
            "\1\132",
            "",
            "\1\133",
            "",
            "\1\134",
            "\1\135",
            "\1\136",
            "",
            "",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\141",
            "\1\142",
            "",
            "\1\143",
            "\1\144",
            "\1\145",
            "\1\146",
            "\1\147",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\151",
            "",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\155",
            "\1\156",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "",
            "\1\161",
            "",
            "",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\163",
            "",
            "",
            "\1\164",
            "",
            "\1\165",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            "\1\167",
            "",
            "\12\41\7\uffff\32\41\4\uffff\1\41\1\uffff\32\41",
            ""
    };

    static final short[] DFA23_eot = DFA.unpackEncodedString(DFA23_eotS);
    static final short[] DFA23_eof = DFA.unpackEncodedString(DFA23_eofS);
    static final char[] DFA23_min = DFA.unpackEncodedStringToUnsignedChars(DFA23_minS);
    static final char[] DFA23_max = DFA.unpackEncodedStringToUnsignedChars(DFA23_maxS);
    static final short[] DFA23_accept = DFA.unpackEncodedString(DFA23_acceptS);
    static final short[] DFA23_special = DFA.unpackEncodedString(DFA23_specialS);
    static final short[][] DFA23_transition;

    static {
        int numStates = DFA23_transitionS.length;
        DFA23_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA23_transition[i] = DFA.unpackEncodedString(DFA23_transitionS[i]);
        }
    }

    class DFA23 extends DFA {

        public DFA23(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 23;
            this.eot = DFA23_eot;
            this.eof = DFA23_eof;
            this.min = DFA23_min;
            this.max = DFA23_max;
            this.accept = DFA23_accept;
            this.special = DFA23_special;
            this.transition = DFA23_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__26 | T__27 | T__28 | T__29 | T__30 | T__31 | T__32 | T__33 | T__34 | T__35 | T__36 | T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | T__58 | T__59 | T__60 | T__61 | T__62 | T__63 | T__64 | T__65 | T__66 | T__67 | T__68 | T__69 | ID | VAR | STAR | COMMENT | WS | STRING | UINT | INTEGER | DECIMAL );";
        }
    }
 

}