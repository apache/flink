// $ANTLR 3.3 Nov 30, 2010 12:50:56 Jaql.g 2011-02-13 16:29:14

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

public class JaqlLexer extends Lexer {
    public static final int EOF=-1;
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
    public static final int SCRIPT=4;
    public static final int ASSIGNMENT=5;
    public static final int JSON_FIELD=6;
    public static final int JSON_OBJECT=7;
    public static final int FUNCTION_CALL=8;
    public static final int OBJECT_EXPR=9;
    public static final int METHOD_CALL=10;
    public static final int FIELD=11;
    public static final int ARRAY_ACCESS=12;
    public static final int ARRAY=13;
    public static final int STREAM=14;
    public static final int BIND=15;
    public static final int OPERATOR=16;
    public static final int VAR=17;
    public static final int ARROW=18;
    public static final int ID=19;
    public static final int INTEGER=20;
    public static final int DECIMAL=21;
    public static final int STRING=22;
    public static final int UINT=23;
    public static final int STAR=24;
    public static final int COMPARISON=25;
    public static final int LETTER=26;
    public static final int DIGIT=27;
    public static final int SIGN=28;
    public static final int COMMENT=29;
    public static final int QUOTE=30;
    public static final int WS=31;
    public static final int UNICODE_ESC=32;
    public static final int OCTAL_ESC=33;
    public static final int ESC_SEQ=34;
    public static final int HEX_DIGIT=35;
    public static final int EXPONENT=36;

    // delegates
    // delegators

    public JaqlLexer() {;} 
    public JaqlLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public JaqlLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "Jaql.g"; }

    // $ANTLR start "T__37"
    public final void mT__37() throws RecognitionException {
        try {
            int _type = T__37;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Jaql.g:3:7: ( ';' )
            // Jaql.g:3:9: ';'
            {
            match(';'); 

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
            // Jaql.g:4:7: ( '=' )
            // Jaql.g:4:9: '='
            {
            match('='); 

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
            // Jaql.g:5:7: ( '(' )
            // Jaql.g:5:9: '('
            {
            match('('); 

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
            // Jaql.g:6:7: ( ')' )
            // Jaql.g:6:9: ')'
            {
            match(')'); 

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
            // Jaql.g:7:7: ( '+' )
            // Jaql.g:7:9: '+'
            {
            match('+'); 

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
            // Jaql.g:8:7: ( '-' )
            // Jaql.g:8:9: '-'
            {
            match('-'); 

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
            // Jaql.g:9:7: ( '/' )
            // Jaql.g:9:9: '/'
            {
            match('/'); 

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
            // Jaql.g:10:7: ( ',' )
            // Jaql.g:10:9: ','
            {
            match(','); 

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
            // Jaql.g:11:7: ( ':' )
            // Jaql.g:11:9: ':'
            {
            match(':'); 

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
            // Jaql.g:12:7: ( '{' )
            // Jaql.g:12:9: '{'
            {
            match('{'); 

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
            // Jaql.g:13:7: ( '}' )
            // Jaql.g:13:9: '}'
            {
            match('}'); 

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
            // Jaql.g:14:7: ( '.' )
            // Jaql.g:14:9: '.'
            {
            match('.'); 

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
            // Jaql.g:15:7: ( '[' )
            // Jaql.g:15:9: '['
            {
            match('['); 

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
            // Jaql.g:16:7: ( ']' )
            // Jaql.g:16:9: ']'
            {
            match(']'); 

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
            // Jaql.g:17:7: ( 'transform' )
            // Jaql.g:17:9: 'transform'
            {
            match("transform"); 


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
            // Jaql.g:18:7: ( 'group by' )
            // Jaql.g:18:9: 'group by'
            {
            match("group by"); 


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
            // Jaql.g:19:7: ( 'into' )
            // Jaql.g:19:9: 'into'
            {
            match("into"); 


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
            // Jaql.g:20:7: ( 'link' )
            // Jaql.g:20:9: 'link'
            {
            match("link"); 


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
            // Jaql.g:21:7: ( 'where' )
            // Jaql.g:21:9: 'where'
            {
            match("where"); 


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
            // Jaql.g:22:7: ( 'partition' )
            // Jaql.g:22:9: 'partition'
            {
            match("partition"); 


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
            // Jaql.g:23:7: ( 'with' )
            // Jaql.g:23:9: 'with'
            {
            match("with"); 


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
            // Jaql.g:24:7: ( 'on' )
            // Jaql.g:24:9: 'on'
            {
            match("on"); 


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
            // Jaql.g:25:7: ( 'in' )
            // Jaql.g:25:9: 'in'
            {
            match("in"); 


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
            // Jaql.g:26:7: ( 'fuse' )
            // Jaql.g:26:9: 'fuse'
            {
            match("fuse"); 


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
            // Jaql.g:27:7: ( 'using' )
            // Jaql.g:27:9: 'using'
            {
            match("using"); 


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
            // Jaql.g:28:7: ( 'weights' )
            // Jaql.g:28:9: 'weights'
            {
            match("weights"); 


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
            // Jaql.g:29:7: ( 'substitute' )
            // Jaql.g:29:9: 'substitute'
            {
            match("substitute"); 


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
            // Jaql.g:30:7: ( 'and' )
            // Jaql.g:30:9: 'and'
            {
            match("and"); 


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
            // Jaql.g:31:7: ( 'or' )
            // Jaql.g:31:9: 'or'
            {
            match("or"); 


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
            // Jaql.g:32:7: ( 'not' )
            // Jaql.g:32:9: 'not'
            {
            match("not"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__66"

    // $ANTLR start "LETTER"
    public final void mLETTER() throws RecognitionException {
        try {
            // Jaql.g:92:2: ( ( 'a' .. 'z' | 'A' .. 'Z' ) )
            // Jaql.g:92:4: ( 'a' .. 'z' | 'A' .. 'Z' )
            {
            if ( (input.LA(1)>='A' && input.LA(1)<='Z')||(input.LA(1)>='a' && input.LA(1)<='z') ) {
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
    // $ANTLR end "LETTER"

    // $ANTLR start "DIGIT"
    public final void mDIGIT() throws RecognitionException {
        try {
            // Jaql.g:94:2: ( '0' .. '9' )
            // Jaql.g:94:4: '0' .. '9'
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
            // Jaql.g:96:2: ( ( '+' | '-' ) )
            // Jaql.g:96:4: ( '+' | '-' )
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
            // Jaql.g:98:4: ( LETTER ( LETTER | DIGIT | '_' )* )
            // Jaql.g:98:6: LETTER ( LETTER | DIGIT | '_' )*
            {
            mLETTER(); 
            // Jaql.g:98:13: ( LETTER | DIGIT | '_' )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')||(LA1_0>='A' && LA1_0<='Z')||LA1_0=='_'||(LA1_0>='a' && LA1_0<='z')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // Jaql.g:
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
            // Jaql.g:101:7: ( '$' ( ID )? )
            // Jaql.g:101:9: '$' ( ID )?
            {
            match('$'); 
            // Jaql.g:101:13: ( ID )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( ((LA2_0>='A' && LA2_0<='Z')||(LA2_0>='a' && LA2_0<='z')) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // Jaql.g:101:13: ID
                    {
                    mID(); 

                    }
                    break;

            }


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
            // Jaql.g:103:6: ( '*' )
            // Jaql.g:103:8: '*'
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
            // Jaql.g:106:5: ( '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' | '/*' ( options {greedy=false; } : . )* '*/' )
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0=='/') ) {
                int LA6_1 = input.LA(2);

                if ( (LA6_1=='/') ) {
                    alt6=1;
                }
                else if ( (LA6_1=='*') ) {
                    alt6=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 6, 1, input);

                    throw nvae;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;
            }
            switch (alt6) {
                case 1 :
                    // Jaql.g:106:9: '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
                    {
                    match("//"); 

                    // Jaql.g:106:14: (~ ( '\\n' | '\\r' ) )*
                    loop3:
                    do {
                        int alt3=2;
                        int LA3_0 = input.LA(1);

                        if ( ((LA3_0>='\u0000' && LA3_0<='\t')||(LA3_0>='\u000B' && LA3_0<='\f')||(LA3_0>='\u000E' && LA3_0<='\uFFFF')) ) {
                            alt3=1;
                        }


                        switch (alt3) {
                    	case 1 :
                    	    // Jaql.g:106:14: ~ ( '\\n' | '\\r' )
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
                    	    break loop3;
                        }
                    } while (true);

                    // Jaql.g:106:28: ( '\\r' )?
                    int alt4=2;
                    int LA4_0 = input.LA(1);

                    if ( (LA4_0=='\r') ) {
                        alt4=1;
                    }
                    switch (alt4) {
                        case 1 :
                            // Jaql.g:106:28: '\\r'
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
                    // Jaql.g:107:9: '/*' ( options {greedy=false; } : . )* '*/'
                    {
                    match("/*"); 

                    // Jaql.g:107:14: ( options {greedy=false; } : . )*
                    loop5:
                    do {
                        int alt5=2;
                        int LA5_0 = input.LA(1);

                        if ( (LA5_0=='*') ) {
                            int LA5_1 = input.LA(2);

                            if ( (LA5_1=='/') ) {
                                alt5=2;
                            }
                            else if ( ((LA5_1>='\u0000' && LA5_1<='.')||(LA5_1>='0' && LA5_1<='\uFFFF')) ) {
                                alt5=1;
                            }


                        }
                        else if ( ((LA5_0>='\u0000' && LA5_0<=')')||(LA5_0>='+' && LA5_0<='\uFFFF')) ) {
                            alt5=1;
                        }


                        switch (alt5) {
                    	case 1 :
                    	    // Jaql.g:107:42: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop5;
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

    // $ANTLR start "QUOTE"
    public final void mQUOTE() throws RecognitionException {
        try {
            // Jaql.g:111:2: ( '\\'' )
            // Jaql.g:111:4: '\\''
            {
            match('\''); 

            }

        }
        finally {
        }
    }
    // $ANTLR end "QUOTE"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Jaql.g:113:5: ( ( ' ' | '\\t' | '\\n' | '\\r' )+ )
            // Jaql.g:113:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            {
            // Jaql.g:113:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
            int cnt7=0;
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( ((LA7_0>='\t' && LA7_0<='\n')||LA7_0=='\r'||LA7_0==' ') ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // Jaql.g:
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
            	    if ( cnt7 >= 1 ) break loop7;
                        EarlyExitException eee =
                            new EarlyExitException(7, input);
                        throw eee;
                }
                cnt7++;
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
            // Jaql.g:116:2: ( QUOTE ( options {greedy=false; } : . )* QUOTE )
            // Jaql.g:116:4: QUOTE ( options {greedy=false; } : . )* QUOTE
            {
            mQUOTE(); 
            // Jaql.g:116:10: ( options {greedy=false; } : . )*
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
            	    // Jaql.g:116:37: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);

            mQUOTE(); 
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
            // Jaql.g:120:5: ( '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' ) | UNICODE_ESC | OCTAL_ESC )
            int alt9=3;
            int LA9_0 = input.LA(1);

            if ( (LA9_0=='\\') ) {
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
                    alt9=1;
                    }
                    break;
                case 'u':
                    {
                    alt9=2;
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
                    alt9=3;
                    }
                    break;
                default:
                    NoViableAltException nvae =
                        new NoViableAltException("", 9, 1, input);

                    throw nvae;
                }

            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 9, 0, input);

                throw nvae;
            }
            switch (alt9) {
                case 1 :
                    // Jaql.g:120:9: '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' )
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
                    // Jaql.g:121:9: UNICODE_ESC
                    {
                    mUNICODE_ESC(); 

                    }
                    break;
                case 3 :
                    // Jaql.g:122:9: OCTAL_ESC
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
            // Jaql.g:127:5: ( '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) )
            int alt10=3;
            int LA10_0 = input.LA(1);

            if ( (LA10_0=='\\') ) {
                int LA10_1 = input.LA(2);

                if ( ((LA10_1>='0' && LA10_1<='3')) ) {
                    int LA10_2 = input.LA(3);

                    if ( ((LA10_2>='0' && LA10_2<='7')) ) {
                        int LA10_4 = input.LA(4);

                        if ( ((LA10_4>='0' && LA10_4<='7')) ) {
                            alt10=1;
                        }
                        else {
                            alt10=2;}
                    }
                    else {
                        alt10=3;}
                }
                else if ( ((LA10_1>='4' && LA10_1<='7')) ) {
                    int LA10_3 = input.LA(3);

                    if ( ((LA10_3>='0' && LA10_3<='7')) ) {
                        alt10=2;
                    }
                    else {
                        alt10=3;}
                }
                else {
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
                    // Jaql.g:127:9: '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // Jaql.g:127:14: ( '0' .. '3' )
                    // Jaql.g:127:15: '0' .. '3'
                    {
                    matchRange('0','3'); 

                    }

                    // Jaql.g:127:25: ( '0' .. '7' )
                    // Jaql.g:127:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // Jaql.g:127:36: ( '0' .. '7' )
                    // Jaql.g:127:37: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 2 :
                    // Jaql.g:128:9: '\\\\' ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 
                    // Jaql.g:128:14: ( '0' .. '7' )
                    // Jaql.g:128:15: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }

                    // Jaql.g:128:25: ( '0' .. '7' )
                    // Jaql.g:128:26: '0' .. '7'
                    {
                    matchRange('0','7'); 

                    }


                    }
                    break;
                case 3 :
                    // Jaql.g:129:9: '\\\\' ( '0' .. '7' )
                    {
                    match('\\'); 
                    // Jaql.g:129:14: ( '0' .. '7' )
                    // Jaql.g:129:15: '0' .. '7'
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
            // Jaql.g:133:13: ( '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT )
            // Jaql.g:133:17: '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
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
            // Jaql.g:136:11: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
            // Jaql.g:136:13: ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' )
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
            // Jaql.g:139:6: ( ( '0' .. '9' )+ )
            // Jaql.g:139:8: ( '0' .. '9' )+
            {
            // Jaql.g:139:8: ( '0' .. '9' )+
            int cnt11=0;
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( ((LA11_0>='0' && LA11_0<='9')) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // Jaql.g:139:8: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt11 >= 1 ) break loop11;
                        EarlyExitException eee =
                            new EarlyExitException(11, input);
                        throw eee;
                }
                cnt11++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "UINT"

    // $ANTLR start "ARROW"
    public final void mARROW() throws RecognitionException {
        try {
            int _type = ARROW;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Jaql.g:142:6: ( '->' )
            // Jaql.g:142:8: '->'
            {
            match("->"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "ARROW"

    // $ANTLR start "INTEGER"
    public final void mINTEGER() throws RecognitionException {
        try {
            int _type = INTEGER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Jaql.g:144:9: ( ( '+' | '-' )? UINT )
            // Jaql.g:144:11: ( '+' | '-' )? UINT
            {
            // Jaql.g:144:11: ( '+' | '-' )?
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0=='+'||LA12_0=='-') ) {
                alt12=1;
            }
            switch (alt12) {
                case 1 :
                    // Jaql.g:
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

    // $ANTLR start "COMPARISON"
    public final void mCOMPARISON() throws RecognitionException {
        try {
            int _type = COMPARISON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Jaql.g:147:2: ( ( '<' | '>' ) ( '=' )? | '!=' | '==' )
            int alt14=3;
            switch ( input.LA(1) ) {
            case '<':
            case '>':
                {
                alt14=1;
                }
                break;
            case '!':
                {
                alt14=2;
                }
                break;
            case '=':
                {
                alt14=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }

            switch (alt14) {
                case 1 :
                    // Jaql.g:147:4: ( '<' | '>' ) ( '=' )?
                    {
                    if ( input.LA(1)=='<'||input.LA(1)=='>' ) {
                        input.consume();

                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;}

                    // Jaql.g:147:16: ( '=' )?
                    int alt13=2;
                    int LA13_0 = input.LA(1);

                    if ( (LA13_0=='=') ) {
                        alt13=1;
                    }
                    switch (alt13) {
                        case 1 :
                            // Jaql.g:147:16: '='
                            {
                            match('='); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // Jaql.g:147:23: '!='
                    {
                    match("!="); 


                    }
                    break;
                case 3 :
                    // Jaql.g:147:30: '=='
                    {
                    match("=="); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "COMPARISON"

    // $ANTLR start "DECIMAL"
    public final void mDECIMAL() throws RecognitionException {
        try {
            int _type = DECIMAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // Jaql.g:150:5: ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT )
            int alt21=3;
            alt21 = dfa21.predict(input);
            switch (alt21) {
                case 1 :
                    // Jaql.g:150:9: ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )?
                    {
                    // Jaql.g:150:9: ( '0' .. '9' )+
                    int cnt15=0;
                    loop15:
                    do {
                        int alt15=2;
                        int LA15_0 = input.LA(1);

                        if ( ((LA15_0>='0' && LA15_0<='9')) ) {
                            alt15=1;
                        }


                        switch (alt15) {
                    	case 1 :
                    	    // Jaql.g:150:10: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt15 >= 1 ) break loop15;
                                EarlyExitException eee =
                                    new EarlyExitException(15, input);
                                throw eee;
                        }
                        cnt15++;
                    } while (true);

                    match('.'); 
                    // Jaql.g:150:25: ( '0' .. '9' )*
                    loop16:
                    do {
                        int alt16=2;
                        int LA16_0 = input.LA(1);

                        if ( ((LA16_0>='0' && LA16_0<='9')) ) {
                            alt16=1;
                        }


                        switch (alt16) {
                    	case 1 :
                    	    // Jaql.g:150:26: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    break loop16;
                        }
                    } while (true);

                    // Jaql.g:150:37: ( EXPONENT )?
                    int alt17=2;
                    int LA17_0 = input.LA(1);

                    if ( (LA17_0=='E'||LA17_0=='e') ) {
                        alt17=1;
                    }
                    switch (alt17) {
                        case 1 :
                            // Jaql.g:150:37: EXPONENT
                            {
                            mEXPONENT(); 

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // Jaql.g:151:9: '.' ( '0' .. '9' )+ ( EXPONENT )?
                    {
                    match('.'); 
                    // Jaql.g:151:13: ( '0' .. '9' )+
                    int cnt18=0;
                    loop18:
                    do {
                        int alt18=2;
                        int LA18_0 = input.LA(1);

                        if ( ((LA18_0>='0' && LA18_0<='9')) ) {
                            alt18=1;
                        }


                        switch (alt18) {
                    	case 1 :
                    	    // Jaql.g:151:14: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt18 >= 1 ) break loop18;
                                EarlyExitException eee =
                                    new EarlyExitException(18, input);
                                throw eee;
                        }
                        cnt18++;
                    } while (true);

                    // Jaql.g:151:25: ( EXPONENT )?
                    int alt19=2;
                    int LA19_0 = input.LA(1);

                    if ( (LA19_0=='E'||LA19_0=='e') ) {
                        alt19=1;
                    }
                    switch (alt19) {
                        case 1 :
                            // Jaql.g:151:25: EXPONENT
                            {
                            mEXPONENT(); 

                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // Jaql.g:152:9: ( '0' .. '9' )+ EXPONENT
                    {
                    // Jaql.g:152:9: ( '0' .. '9' )+
                    int cnt20=0;
                    loop20:
                    do {
                        int alt20=2;
                        int LA20_0 = input.LA(1);

                        if ( ((LA20_0>='0' && LA20_0<='9')) ) {
                            alt20=1;
                        }


                        switch (alt20) {
                    	case 1 :
                    	    // Jaql.g:152:10: '0' .. '9'
                    	    {
                    	    matchRange('0','9'); 

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt20 >= 1 ) break loop20;
                                EarlyExitException eee =
                                    new EarlyExitException(20, input);
                                throw eee;
                        }
                        cnt20++;
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
            // Jaql.g:155:10: ( ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ )
            // Jaql.g:155:12: ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // Jaql.g:155:22: ( '+' | '-' )?
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( (LA22_0=='+'||LA22_0=='-') ) {
                alt22=1;
            }
            switch (alt22) {
                case 1 :
                    // Jaql.g:
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

            // Jaql.g:155:33: ( '0' .. '9' )+
            int cnt23=0;
            loop23:
            do {
                int alt23=2;
                int LA23_0 = input.LA(1);

                if ( ((LA23_0>='0' && LA23_0<='9')) ) {
                    alt23=1;
                }


                switch (alt23) {
            	case 1 :
            	    // Jaql.g:155:34: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt23 >= 1 ) break loop23;
                        EarlyExitException eee =
                            new EarlyExitException(23, input);
                        throw eee;
                }
                cnt23++;
            } while (true);


            }

        }
        finally {
        }
    }
    // $ANTLR end "EXPONENT"

    public void mTokens() throws RecognitionException {
        // Jaql.g:1:8: ( T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | T__58 | T__59 | T__60 | T__61 | T__62 | T__63 | T__64 | T__65 | T__66 | ID | VAR | STAR | COMMENT | WS | STRING | UINT | ARROW | INTEGER | COMPARISON | DECIMAL )
        int alt24=41;
        alt24 = dfa24.predict(input);
        switch (alt24) {
            case 1 :
                // Jaql.g:1:10: T__37
                {
                mT__37(); 

                }
                break;
            case 2 :
                // Jaql.g:1:16: T__38
                {
                mT__38(); 

                }
                break;
            case 3 :
                // Jaql.g:1:22: T__39
                {
                mT__39(); 

                }
                break;
            case 4 :
                // Jaql.g:1:28: T__40
                {
                mT__40(); 

                }
                break;
            case 5 :
                // Jaql.g:1:34: T__41
                {
                mT__41(); 

                }
                break;
            case 6 :
                // Jaql.g:1:40: T__42
                {
                mT__42(); 

                }
                break;
            case 7 :
                // Jaql.g:1:46: T__43
                {
                mT__43(); 

                }
                break;
            case 8 :
                // Jaql.g:1:52: T__44
                {
                mT__44(); 

                }
                break;
            case 9 :
                // Jaql.g:1:58: T__45
                {
                mT__45(); 

                }
                break;
            case 10 :
                // Jaql.g:1:64: T__46
                {
                mT__46(); 

                }
                break;
            case 11 :
                // Jaql.g:1:70: T__47
                {
                mT__47(); 

                }
                break;
            case 12 :
                // Jaql.g:1:76: T__48
                {
                mT__48(); 

                }
                break;
            case 13 :
                // Jaql.g:1:82: T__49
                {
                mT__49(); 

                }
                break;
            case 14 :
                // Jaql.g:1:88: T__50
                {
                mT__50(); 

                }
                break;
            case 15 :
                // Jaql.g:1:94: T__51
                {
                mT__51(); 

                }
                break;
            case 16 :
                // Jaql.g:1:100: T__52
                {
                mT__52(); 

                }
                break;
            case 17 :
                // Jaql.g:1:106: T__53
                {
                mT__53(); 

                }
                break;
            case 18 :
                // Jaql.g:1:112: T__54
                {
                mT__54(); 

                }
                break;
            case 19 :
                // Jaql.g:1:118: T__55
                {
                mT__55(); 

                }
                break;
            case 20 :
                // Jaql.g:1:124: T__56
                {
                mT__56(); 

                }
                break;
            case 21 :
                // Jaql.g:1:130: T__57
                {
                mT__57(); 

                }
                break;
            case 22 :
                // Jaql.g:1:136: T__58
                {
                mT__58(); 

                }
                break;
            case 23 :
                // Jaql.g:1:142: T__59
                {
                mT__59(); 

                }
                break;
            case 24 :
                // Jaql.g:1:148: T__60
                {
                mT__60(); 

                }
                break;
            case 25 :
                // Jaql.g:1:154: T__61
                {
                mT__61(); 

                }
                break;
            case 26 :
                // Jaql.g:1:160: T__62
                {
                mT__62(); 

                }
                break;
            case 27 :
                // Jaql.g:1:166: T__63
                {
                mT__63(); 

                }
                break;
            case 28 :
                // Jaql.g:1:172: T__64
                {
                mT__64(); 

                }
                break;
            case 29 :
                // Jaql.g:1:178: T__65
                {
                mT__65(); 

                }
                break;
            case 30 :
                // Jaql.g:1:184: T__66
                {
                mT__66(); 

                }
                break;
            case 31 :
                // Jaql.g:1:190: ID
                {
                mID(); 

                }
                break;
            case 32 :
                // Jaql.g:1:193: VAR
                {
                mVAR(); 

                }
                break;
            case 33 :
                // Jaql.g:1:197: STAR
                {
                mSTAR(); 

                }
                break;
            case 34 :
                // Jaql.g:1:202: COMMENT
                {
                mCOMMENT(); 

                }
                break;
            case 35 :
                // Jaql.g:1:210: WS
                {
                mWS(); 

                }
                break;
            case 36 :
                // Jaql.g:1:213: STRING
                {
                mSTRING(); 

                }
                break;
            case 37 :
                // Jaql.g:1:220: UINT
                {
                mUINT(); 

                }
                break;
            case 38 :
                // Jaql.g:1:225: ARROW
                {
                mARROW(); 

                }
                break;
            case 39 :
                // Jaql.g:1:231: INTEGER
                {
                mINTEGER(); 

                }
                break;
            case 40 :
                // Jaql.g:1:239: COMPARISON
                {
                mCOMPARISON(); 

                }
                break;
            case 41 :
                // Jaql.g:1:250: DECIMAL
                {
                mDECIMAL(); 

                }
                break;

        }

    }


    protected DFA21 dfa21 = new DFA21(this);
    protected DFA24 dfa24 = new DFA24(this);
    static final String DFA21_eotS =
        "\5\uffff";
    static final String DFA21_eofS =
        "\5\uffff";
    static final String DFA21_minS =
        "\2\56\3\uffff";
    static final String DFA21_maxS =
        "\1\71\1\145\3\uffff";
    static final String DFA21_acceptS =
        "\2\uffff\1\2\1\1\1\3";
    static final String DFA21_specialS =
        "\5\uffff}>";
    static final String[] DFA21_transitionS = {
            "\1\2\1\uffff\12\1",
            "\1\3\1\uffff\12\1\13\uffff\1\4\37\uffff\1\4",
            "",
            "",
            ""
    };

    static final short[] DFA21_eot = DFA.unpackEncodedString(DFA21_eotS);
    static final short[] DFA21_eof = DFA.unpackEncodedString(DFA21_eofS);
    static final char[] DFA21_min = DFA.unpackEncodedStringToUnsignedChars(DFA21_minS);
    static final char[] DFA21_max = DFA.unpackEncodedStringToUnsignedChars(DFA21_maxS);
    static final short[] DFA21_accept = DFA.unpackEncodedString(DFA21_acceptS);
    static final short[] DFA21_special = DFA.unpackEncodedString(DFA21_specialS);
    static final short[][] DFA21_transition;

    static {
        int numStates = DFA21_transitionS.length;
        DFA21_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA21_transition[i] = DFA.unpackEncodedString(DFA21_transitionS[i]);
        }
    }

    class DFA21 extends DFA {

        public DFA21(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 21;
            this.eot = DFA21_eot;
            this.eof = DFA21_eof;
            this.min = DFA21_min;
            this.max = DFA21_max;
            this.accept = DFA21_accept;
            this.special = DFA21_special;
            this.transition = DFA21_transition;
        }
        public String getDescription() {
            return "149:1: DECIMAL : ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT );";
        }
    }
    static final String DFA24_eotS =
        "\2\uffff\1\42\2\uffff\1\43\1\46\1\50\4\uffff\1\51\2\uffff\14\33"+
        "\5\uffff\1\72\12\uffff\2\33\1\76\5\33\1\104\1\105\5\33\1\uffff\3"+
        "\33\1\uffff\5\33\2\uffff\3\33\1\126\1\127\2\33\1\132\1\133\1\33"+
        "\1\135\2\33\1\140\2\33\2\uffff\2\33\2\uffff\1\145\1\uffff\2\33\1"+
        "\uffff\1\150\2\33\2\uffff\2\33\1\uffff\2\33\1\157\3\33\1\uffff\2"+
        "\33\1\165\1\166\1\33\2\uffff\1\170\1\uffff";
    static final String DFA24_eofS =
        "\171\uffff";
    static final String DFA24_minS =
        "\1\11\1\uffff\1\75\2\uffff\2\60\1\52\4\uffff\1\60\2\uffff\2\162"+
        "\1\156\1\151\1\145\1\141\1\156\1\165\1\163\1\165\1\156\1\157\5\uffff"+
        "\1\56\12\uffff\1\141\1\157\1\60\1\156\1\145\1\164\1\151\1\162\2"+
        "\60\1\163\1\151\1\142\1\144\1\164\1\uffff\1\156\1\165\1\157\1\uffff"+
        "\1\153\1\162\1\150\1\147\1\164\2\uffff\1\145\1\156\1\163\2\60\1"+
        "\163\1\160\2\60\1\145\1\60\1\150\1\151\1\60\1\147\1\164\2\uffff"+
        "\1\146\1\40\2\uffff\1\60\1\uffff\2\164\1\uffff\1\60\1\151\1\157"+
        "\2\uffff\1\163\1\151\1\uffff\1\164\1\162\1\60\1\157\1\165\1\155"+
        "\1\uffff\1\156\1\164\2\60\1\145\2\uffff\1\60\1\uffff";
    static final String DFA24_maxS =
        "\1\175\1\uffff\1\75\2\uffff\1\71\1\76\1\57\4\uffff\1\71\2\uffff"+
        "\2\162\1\156\2\151\1\141\1\162\1\165\1\163\1\165\1\156\1\157\5\uffff"+
        "\1\145\12\uffff\1\141\1\157\1\172\1\156\1\145\1\164\1\151\1\162"+
        "\2\172\1\163\1\151\1\142\1\144\1\164\1\uffff\1\156\1\165\1\157\1"+
        "\uffff\1\153\1\162\1\150\1\147\1\164\2\uffff\1\145\1\156\1\163\2"+
        "\172\1\163\1\160\2\172\1\145\1\172\1\150\1\151\1\172\1\147\1\164"+
        "\2\uffff\1\146\1\40\2\uffff\1\172\1\uffff\2\164\1\uffff\1\172\1"+
        "\151\1\157\2\uffff\1\163\1\151\1\uffff\1\164\1\162\1\172\1\157\1"+
        "\165\1\155\1\uffff\1\156\1\164\2\172\1\145\2\uffff\1\172\1\uffff";
    static final String DFA24_acceptS =
        "\1\uffff\1\1\1\uffff\1\3\1\4\3\uffff\1\10\1\11\1\12\1\13\1\uffff"+
        "\1\15\1\16\14\uffff\1\37\1\40\1\41\1\43\1\44\1\uffff\1\50\1\2\1"+
        "\5\1\47\1\46\1\6\1\42\1\7\1\14\1\51\17\uffff\1\45\3\uffff\1\27\5"+
        "\uffff\1\26\1\35\20\uffff\1\34\1\36\2\uffff\1\21\1\22\1\uffff\1"+
        "\25\2\uffff\1\30\3\uffff\1\20\1\23\2\uffff\1\31\6\uffff\1\32\5\uffff"+
        "\1\17\1\24\1\uffff\1\33";
    static final String DFA24_specialS =
        "\171\uffff}>";
    static final String[] DFA24_transitionS = {
            "\2\36\2\uffff\1\36\22\uffff\1\36\1\41\2\uffff\1\34\2\uffff\1"+
            "\37\1\3\1\4\1\35\1\5\1\10\1\6\1\14\1\7\12\40\1\11\1\1\1\41\1"+
            "\2\1\41\2\uffff\32\33\1\15\1\uffff\1\16\3\uffff\1\31\4\33\1"+
            "\26\1\20\1\33\1\21\2\33\1\22\1\33\1\32\1\25\1\24\2\33\1\30\1"+
            "\17\1\27\1\33\1\23\3\33\1\12\1\uffff\1\13",
            "",
            "\1\41",
            "",
            "",
            "\12\44",
            "\12\44\4\uffff\1\45",
            "\1\47\4\uffff\1\47",
            "",
            "",
            "",
            "",
            "\12\52",
            "",
            "",
            "\1\53",
            "\1\54",
            "\1\55",
            "\1\56",
            "\1\61\2\uffff\1\57\1\60",
            "\1\62",
            "\1\63\3\uffff\1\64",
            "\1\65",
            "\1\66",
            "\1\67",
            "\1\70",
            "\1\71",
            "",
            "",
            "",
            "",
            "",
            "\1\52\1\uffff\12\40\13\uffff\1\52\37\uffff\1\52",
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
            "\1\73",
            "\1\74",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\23\33\1\75\6\33",
            "\1\77",
            "\1\100",
            "\1\101",
            "\1\102",
            "\1\103",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\106",
            "\1\107",
            "\1\110",
            "\1\111",
            "\1\112",
            "",
            "\1\113",
            "\1\114",
            "\1\115",
            "",
            "\1\116",
            "\1\117",
            "\1\120",
            "\1\121",
            "\1\122",
            "",
            "",
            "\1\123",
            "\1\124",
            "\1\125",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\130",
            "\1\131",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\134",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\136",
            "\1\137",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\141",
            "\1\142",
            "",
            "",
            "\1\143",
            "\1\144",
            "",
            "",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "",
            "\1\146",
            "\1\147",
            "",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\151",
            "\1\152",
            "",
            "",
            "\1\153",
            "\1\154",
            "",
            "\1\155",
            "\1\156",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\160",
            "\1\161",
            "\1\162",
            "",
            "\1\163",
            "\1\164",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            "\1\167",
            "",
            "",
            "\12\33\7\uffff\32\33\4\uffff\1\33\1\uffff\32\33",
            ""
    };

    static final short[] DFA24_eot = DFA.unpackEncodedString(DFA24_eotS);
    static final short[] DFA24_eof = DFA.unpackEncodedString(DFA24_eofS);
    static final char[] DFA24_min = DFA.unpackEncodedStringToUnsignedChars(DFA24_minS);
    static final char[] DFA24_max = DFA.unpackEncodedStringToUnsignedChars(DFA24_maxS);
    static final short[] DFA24_accept = DFA.unpackEncodedString(DFA24_acceptS);
    static final short[] DFA24_special = DFA.unpackEncodedString(DFA24_specialS);
    static final short[][] DFA24_transition;

    static {
        int numStates = DFA24_transitionS.length;
        DFA24_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA24_transition[i] = DFA.unpackEncodedString(DFA24_transitionS[i]);
        }
    }

    class DFA24 extends DFA {

        public DFA24(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 24;
            this.eot = DFA24_eot;
            this.eof = DFA24_eof;
            this.min = DFA24_min;
            this.max = DFA24_max;
            this.accept = DFA24_accept;
            this.special = DFA24_special;
            this.transition = DFA24_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__37 | T__38 | T__39 | T__40 | T__41 | T__42 | T__43 | T__44 | T__45 | T__46 | T__47 | T__48 | T__49 | T__50 | T__51 | T__52 | T__53 | T__54 | T__55 | T__56 | T__57 | T__58 | T__59 | T__60 | T__61 | T__62 | T__63 | T__64 | T__65 | T__66 | ID | VAR | STAR | COMMENT | WS | STRING | UINT | ARROW | INTEGER | COMPARISON | DECIMAL );";
        }
    }
 

}