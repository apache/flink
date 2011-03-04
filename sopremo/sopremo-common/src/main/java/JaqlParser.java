// $ANTLR 3.3 Nov 30, 2010 12:50:56 Jaql.g 2011-02-13 16:29:13

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;


import org.antlr.runtime.tree.*;

public class JaqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "SCRIPT", "ASSIGNMENT", "JSON_FIELD", "JSON_OBJECT", "FUNCTION_CALL", "OBJECT_EXPR", "METHOD_CALL", "FIELD", "ARRAY_ACCESS", "ARRAY", "STREAM", "BIND", "OPERATOR", "VAR", "ARROW", "ID", "INTEGER", "DECIMAL", "STRING", "UINT", "STAR", "COMPARISON", "LETTER", "DIGIT", "SIGN", "COMMENT", "QUOTE", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'='", "'('", "')'", "'+'", "'-'", "'/'", "','", "':'", "'{'", "'}'", "'.'", "'['", "']'", "'transform'", "'group by'", "'into'", "'link'", "'where'", "'partition'", "'with'", "'on'", "'in'", "'fuse'", "'using'", "'weights'", "'substitute'", "'and'", "'or'", "'not'"
    };
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


        public JaqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public JaqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return JaqlParser.tokenNames; }
    public String getGrammarFileName() { return "Jaql.g"; }


    public static class script_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "script"
    // Jaql.g:25:1: script : ( statement )* -> ^( SCRIPT ( statement )* ) ;
    public final JaqlParser.script_return script() throws RecognitionException {
        JaqlParser.script_return retval = new JaqlParser.script_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        JaqlParser.statement_return statement1 = null;


        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            // Jaql.g:25:8: ( ( statement )* -> ^( SCRIPT ( statement )* ) )
            // Jaql.g:25:10: ( statement )*
            {
            // Jaql.g:25:10: ( statement )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==VAR||LA1_0==ID||(LA1_0>=51 && LA1_0<=52)||LA1_0==54||LA1_0==60||LA1_0==63) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // Jaql.g:25:10: statement
            	    {
            	    pushFollow(FOLLOW_statement_in_script139);
            	    statement1=statement();

            	    state._fsp--;

            	    stream_statement.add(statement1.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);



            // AST REWRITE
            // elements: statement
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 25:21: -> ^( SCRIPT ( statement )* )
            {
                // Jaql.g:25:24: ^( SCRIPT ( statement )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(SCRIPT, "SCRIPT"), root_1);

                // Jaql.g:25:33: ( statement )*
                while ( stream_statement.hasNext() ) {
                    adaptor.addChild(root_1, stream_statement.nextTree());

                }
                stream_statement.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "script"

    public static class statement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "statement"
    // Jaql.g:27:1: statement : ( assignment | streamStart ) ';' ;
    public final JaqlParser.statement_return statement() throws RecognitionException {
        JaqlParser.statement_return retval = new JaqlParser.statement_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal4=null;
        JaqlParser.assignment_return assignment2 = null;

        JaqlParser.streamStart_return streamStart3 = null;


        CommonTree char_literal4_tree=null;

        try {
            // Jaql.g:27:11: ( ( assignment | streamStart ) ';' )
            // Jaql.g:27:13: ( assignment | streamStart ) ';'
            {
            root_0 = (CommonTree)adaptor.nil();

            // Jaql.g:27:13: ( assignment | streamStart )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==VAR) ) {
                int LA2_1 = input.LA(2);

                if ( (LA2_1==38) ) {
                    alt2=1;
                }
                else if ( (LA2_1==ARROW||LA2_1==37) ) {
                    alt2=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA2_0==ID||(LA2_0>=51 && LA2_0<=52)||LA2_0==54||LA2_0==60||LA2_0==63) ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // Jaql.g:27:14: assignment
                    {
                    pushFollow(FOLLOW_assignment_in_statement158);
                    assignment2=assignment();

                    state._fsp--;

                    adaptor.addChild(root_0, assignment2.getTree());

                    }
                    break;
                case 2 :
                    // Jaql.g:27:27: streamStart
                    {
                    pushFollow(FOLLOW_streamStart_in_statement162);
                    streamStart3=streamStart();

                    state._fsp--;

                    adaptor.addChild(root_0, streamStart3.getTree());

                    }
                    break;

            }

            char_literal4=(Token)match(input,37,FOLLOW_37_in_statement165); 

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "statement"

    public static class expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expr"
    // Jaql.g:29:1: expr : ( functionCall | operator );
    public final JaqlParser.expr_return expr() throws RecognitionException {
        JaqlParser.expr_return retval = new JaqlParser.expr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        JaqlParser.functionCall_return functionCall5 = null;

        JaqlParser.operator_return operator6 = null;



        try {
            // Jaql.g:29:7: ( functionCall | operator )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==ID) ) {
                alt3=1;
            }
            else if ( ((LA3_0>=51 && LA3_0<=52)||LA3_0==54||LA3_0==60||LA3_0==63) ) {
                alt3=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // Jaql.g:29:9: functionCall
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_functionCall_in_expr177);
                    functionCall5=functionCall();

                    state._fsp--;

                    adaptor.addChild(root_0, functionCall5.getTree());

                    }
                    break;
                case 2 :
                    // Jaql.g:29:24: operator
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_operator_in_expr181);
                    operator6=operator();

                    state._fsp--;

                    adaptor.addChild(root_0, operator6.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "expr"

    public static class streamStart_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "streamStart"
    // Jaql.g:31:1: streamStart : ( VAR | expr ) (s= stream )? -> ^( STREAM ( VAR )? ( expr )? ( $s)? ) ;
    public final JaqlParser.streamStart_return streamStart() throws RecognitionException {
        JaqlParser.streamStart_return retval = new JaqlParser.streamStart_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token VAR7=null;
        JaqlParser.stream_return s = null;

        JaqlParser.expr_return expr8 = null;


        CommonTree VAR7_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleSubtreeStream stream_stream=new RewriteRuleSubtreeStream(adaptor,"rule stream");
        RewriteRuleSubtreeStream stream_expr=new RewriteRuleSubtreeStream(adaptor,"rule expr");
        try {
            // Jaql.g:31:13: ( ( VAR | expr ) (s= stream )? -> ^( STREAM ( VAR )? ( expr )? ( $s)? ) )
            // Jaql.g:31:15: ( VAR | expr ) (s= stream )?
            {
            // Jaql.g:31:15: ( VAR | expr )
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==VAR) ) {
                alt4=1;
            }
            else if ( (LA4_0==ID||(LA4_0>=51 && LA4_0<=52)||LA4_0==54||LA4_0==60||LA4_0==63) ) {
                alt4=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;
            }
            switch (alt4) {
                case 1 :
                    // Jaql.g:31:16: VAR
                    {
                    VAR7=(Token)match(input,VAR,FOLLOW_VAR_in_streamStart190);  
                    stream_VAR.add(VAR7);


                    }
                    break;
                case 2 :
                    // Jaql.g:31:22: expr
                    {
                    pushFollow(FOLLOW_expr_in_streamStart194);
                    expr8=expr();

                    state._fsp--;

                    stream_expr.add(expr8.getTree());

                    }
                    break;

            }

            // Jaql.g:31:29: (s= stream )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==ARROW) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // Jaql.g:31:29: s= stream
                    {
                    pushFollow(FOLLOW_stream_in_streamStart199);
                    s=stream();

                    state._fsp--;

                    stream_stream.add(s.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: expr, VAR, s
            // token labels: 
            // rule labels: retval, s
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_s=new RewriteRuleSubtreeStream(adaptor,"rule s",s!=null?s.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 31:38: -> ^( STREAM ( VAR )? ( expr )? ( $s)? )
            {
                // Jaql.g:31:41: ^( STREAM ( VAR )? ( expr )? ( $s)? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(STREAM, "STREAM"), root_1);

                // Jaql.g:31:50: ( VAR )?
                if ( stream_VAR.hasNext() ) {
                    adaptor.addChild(root_1, stream_VAR.nextNode());

                }
                stream_VAR.reset();
                // Jaql.g:31:55: ( expr )?
                if ( stream_expr.hasNext() ) {
                    adaptor.addChild(root_1, stream_expr.nextTree());

                }
                stream_expr.reset();
                // Jaql.g:31:61: ( $s)?
                if ( stream_s.hasNext() ) {
                    adaptor.addChild(root_1, stream_s.nextTree());

                }
                stream_s.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "streamStart"

    public static class stream_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "stream"
    // Jaql.g:33:1: stream : ARROW expr ( stream )? ;
    public final JaqlParser.stream_return stream() throws RecognitionException {
        JaqlParser.stream_return retval = new JaqlParser.stream_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ARROW9=null;
        JaqlParser.expr_return expr10 = null;

        JaqlParser.stream_return stream11 = null;


        CommonTree ARROW9_tree=null;

        try {
            // Jaql.g:33:8: ( ARROW expr ( stream )? )
            // Jaql.g:33:10: ARROW expr ( stream )?
            {
            root_0 = (CommonTree)adaptor.nil();

            ARROW9=(Token)match(input,ARROW,FOLLOW_ARROW_in_stream224); 
            pushFollow(FOLLOW_expr_in_stream227);
            expr10=expr();

            state._fsp--;

            adaptor.addChild(root_0, expr10.getTree());
            // Jaql.g:33:22: ( stream )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==ARROW) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // Jaql.g:33:22: stream
                    {
                    pushFollow(FOLLOW_stream_in_stream229);
                    stream11=stream();

                    state._fsp--;

                    adaptor.addChild(root_0, stream11.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "stream"

    public static class assignment_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "assignment"
    // Jaql.g:35:1: assignment : VAR '=' streamStart -> ^( ASSIGNMENT VAR streamStart ) ;
    public final JaqlParser.assignment_return assignment() throws RecognitionException {
        JaqlParser.assignment_return retval = new JaqlParser.assignment_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token VAR12=null;
        Token char_literal13=null;
        JaqlParser.streamStart_return streamStart14 = null;


        CommonTree VAR12_tree=null;
        CommonTree char_literal13_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_streamStart=new RewriteRuleSubtreeStream(adaptor,"rule streamStart");
        try {
            // Jaql.g:35:12: ( VAR '=' streamStart -> ^( ASSIGNMENT VAR streamStart ) )
            // Jaql.g:35:14: VAR '=' streamStart
            {
            VAR12=(Token)match(input,VAR,FOLLOW_VAR_in_assignment238);  
            stream_VAR.add(VAR12);

            char_literal13=(Token)match(input,38,FOLLOW_38_in_assignment240);  
            stream_38.add(char_literal13);

            pushFollow(FOLLOW_streamStart_in_assignment242);
            streamStart14=streamStart();

            state._fsp--;

            stream_streamStart.add(streamStart14.getTree());


            // AST REWRITE
            // elements: streamStart, VAR
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 35:34: -> ^( ASSIGNMENT VAR streamStart )
            {
                // Jaql.g:35:37: ^( ASSIGNMENT VAR streamStart )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ASSIGNMENT, "ASSIGNMENT"), root_1);

                adaptor.addChild(root_1, stream_VAR.nextNode());
                adaptor.addChild(root_1, stream_streamStart.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "assignment"

    public static class functionCall_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionCall"
    // Jaql.g:37:1: functionCall : ID '(' ( params )? ')' -> ^( FUNCTION_CALL ID ( params )? ) ;
    public final JaqlParser.functionCall_return functionCall() throws RecognitionException {
        JaqlParser.functionCall_return retval = new JaqlParser.functionCall_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID15=null;
        Token char_literal16=null;
        Token char_literal18=null;
        JaqlParser.params_return params17 = null;


        CommonTree ID15_tree=null;
        CommonTree char_literal16_tree=null;
        CommonTree char_literal18_tree=null;
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"rule params");
        try {
            // Jaql.g:37:14: ( ID '(' ( params )? ')' -> ^( FUNCTION_CALL ID ( params )? ) )
            // Jaql.g:37:16: ID '(' ( params )? ')'
            {
            ID15=(Token)match(input,ID,FOLLOW_ID_in_functionCall260);  
            stream_ID.add(ID15);

            char_literal16=(Token)match(input,39,FOLLOW_39_in_functionCall262);  
            stream_39.add(char_literal16);

            // Jaql.g:37:23: ( params )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==VAR||(LA7_0>=ID && LA7_0<=UINT)) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // Jaql.g:37:23: params
                    {
                    pushFollow(FOLLOW_params_in_functionCall264);
                    params17=params();

                    state._fsp--;

                    stream_params.add(params17.getTree());

                    }
                    break;

            }

            char_literal18=(Token)match(input,40,FOLLOW_40_in_functionCall267);  
            stream_40.add(char_literal18);



            // AST REWRITE
            // elements: params, ID
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 37:35: -> ^( FUNCTION_CALL ID ( params )? )
            {
                // Jaql.g:37:38: ^( FUNCTION_CALL ID ( params )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(FUNCTION_CALL, "FUNCTION_CALL"), root_1);

                adaptor.addChild(root_1, stream_ID.nextNode());
                // Jaql.g:37:57: ( params )?
                if ( stream_params.hasNext() ) {
                    adaptor.addChild(root_1, stream_params.nextTree());

                }
                stream_params.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "functionCall"

    public static class arithmExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arithmExpr"
    // Jaql.g:39:1: arithmExpr : ( object | literal ) ( ( '+' | '-' | '*' | '/' ) arithmExpr )? ;
    public final JaqlParser.arithmExpr_return arithmExpr() throws RecognitionException {
        JaqlParser.arithmExpr_return retval = new JaqlParser.arithmExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set21=null;
        JaqlParser.object_return object19 = null;

        JaqlParser.literal_return literal20 = null;

        JaqlParser.arithmExpr_return arithmExpr22 = null;


        CommonTree set21_tree=null;

        try {
            // Jaql.g:39:12: ( ( object | literal ) ( ( '+' | '-' | '*' | '/' ) arithmExpr )? )
            // Jaql.g:39:14: ( object | literal ) ( ( '+' | '-' | '*' | '/' ) arithmExpr )?
            {
            root_0 = (CommonTree)adaptor.nil();

            // Jaql.g:39:14: ( object | literal )
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==VAR||LA8_0==ID) ) {
                alt8=1;
            }
            else if ( ((LA8_0>=INTEGER && LA8_0<=UINT)) ) {
                alt8=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 8, 0, input);

                throw nvae;
            }
            switch (alt8) {
                case 1 :
                    // Jaql.g:39:15: object
                    {
                    pushFollow(FOLLOW_object_in_arithmExpr287);
                    object19=object();

                    state._fsp--;

                    adaptor.addChild(root_0, object19.getTree());

                    }
                    break;
                case 2 :
                    // Jaql.g:39:24: literal
                    {
                    pushFollow(FOLLOW_literal_in_arithmExpr291);
                    literal20=literal();

                    state._fsp--;

                    adaptor.addChild(root_0, literal20.getTree());

                    }
                    break;

            }

            // Jaql.g:39:33: ( ( '+' | '-' | '*' | '/' ) arithmExpr )?
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==STAR||(LA9_0>=41 && LA9_0<=43)) ) {
                alt9=1;
            }
            switch (alt9) {
                case 1 :
                    // Jaql.g:39:34: ( '+' | '-' | '*' | '/' ) arithmExpr
                    {
                    set21=(Token)input.LT(1);
                    if ( input.LA(1)==STAR||(input.LA(1)>=41 && input.LA(1)<=43) ) {
                        input.consume();
                        adaptor.addChild(root_0, (CommonTree)adaptor.create(set21));
                        state.errorRecovery=false;
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }

                    pushFollow(FOLLOW_arithmExpr_in_arithmExpr311);
                    arithmExpr22=arithmExpr();

                    state._fsp--;

                    adaptor.addChild(root_0, arithmExpr22.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "arithmExpr"

    public static class params_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "params"
    // Jaql.g:41:1: params : arithmExpr ( ',' arithmExpr )* ;
    public final JaqlParser.params_return params() throws RecognitionException {
        JaqlParser.params_return retval = new JaqlParser.params_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal24=null;
        JaqlParser.arithmExpr_return arithmExpr23 = null;

        JaqlParser.arithmExpr_return arithmExpr25 = null;


        CommonTree char_literal24_tree=null;

        try {
            // Jaql.g:41:9: ( arithmExpr ( ',' arithmExpr )* )
            // Jaql.g:41:11: arithmExpr ( ',' arithmExpr )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_arithmExpr_in_params322);
            arithmExpr23=arithmExpr();

            state._fsp--;

            adaptor.addChild(root_0, arithmExpr23.getTree());
            // Jaql.g:41:22: ( ',' arithmExpr )*
            loop10:
            do {
                int alt10=2;
                int LA10_0 = input.LA(1);

                if ( (LA10_0==44) ) {
                    alt10=1;
                }


                switch (alt10) {
            	case 1 :
            	    // Jaql.g:41:23: ',' arithmExpr
            	    {
            	    char_literal24=(Token)match(input,44,FOLLOW_44_in_params325); 
            	    pushFollow(FOLLOW_arithmExpr_in_params328);
            	    arithmExpr25=arithmExpr();

            	    state._fsp--;

            	    adaptor.addChild(root_0, arithmExpr25.getTree());

            	    }
            	    break;

            	default :
            	    break loop10;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "params"

    public static class jsonExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "jsonExpr"
    // Jaql.g:43:1: jsonExpr : ( ID ':' )? ( jsonObject | arithmExpr | arrayDefinition ) -> ^( JSON_FIELD ( ID )? ( jsonObject )? ( arithmExpr )? ( arrayDefinition )? ) ;
    public final JaqlParser.jsonExpr_return jsonExpr() throws RecognitionException {
        JaqlParser.jsonExpr_return retval = new JaqlParser.jsonExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID26=null;
        Token char_literal27=null;
        JaqlParser.jsonObject_return jsonObject28 = null;

        JaqlParser.arithmExpr_return arithmExpr29 = null;

        JaqlParser.arrayDefinition_return arrayDefinition30 = null;


        CommonTree ID26_tree=null;
        CommonTree char_literal27_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_jsonObject=new RewriteRuleSubtreeStream(adaptor,"rule jsonObject");
        RewriteRuleSubtreeStream stream_arrayDefinition=new RewriteRuleSubtreeStream(adaptor,"rule arrayDefinition");
        RewriteRuleSubtreeStream stream_arithmExpr=new RewriteRuleSubtreeStream(adaptor,"rule arithmExpr");
        try {
            // Jaql.g:43:10: ( ( ID ':' )? ( jsonObject | arithmExpr | arrayDefinition ) -> ^( JSON_FIELD ( ID )? ( jsonObject )? ( arithmExpr )? ( arrayDefinition )? ) )
            // Jaql.g:43:13: ( ID ':' )? ( jsonObject | arithmExpr | arrayDefinition )
            {
            // Jaql.g:43:13: ( ID ':' )?
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==ID) ) {
                int LA11_1 = input.LA(2);

                if ( (LA11_1==45) ) {
                    alt11=1;
                }
            }
            switch (alt11) {
                case 1 :
                    // Jaql.g:43:14: ID ':'
                    {
                    ID26=(Token)match(input,ID,FOLLOW_ID_in_jsonExpr340);  
                    stream_ID.add(ID26);

                    char_literal27=(Token)match(input,45,FOLLOW_45_in_jsonExpr342);  
                    stream_45.add(char_literal27);


                    }
                    break;

            }

            // Jaql.g:43:23: ( jsonObject | arithmExpr | arrayDefinition )
            int alt12=3;
            switch ( input.LA(1) ) {
            case 46:
                {
                alt12=1;
                }
                break;
            case VAR:
            case ID:
            case INTEGER:
            case DECIMAL:
            case STRING:
            case UINT:
                {
                alt12=2;
                }
                break;
            case 49:
                {
                alt12=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }

            switch (alt12) {
                case 1 :
                    // Jaql.g:43:24: jsonObject
                    {
                    pushFollow(FOLLOW_jsonObject_in_jsonExpr347);
                    jsonObject28=jsonObject();

                    state._fsp--;

                    stream_jsonObject.add(jsonObject28.getTree());

                    }
                    break;
                case 2 :
                    // Jaql.g:43:37: arithmExpr
                    {
                    pushFollow(FOLLOW_arithmExpr_in_jsonExpr351);
                    arithmExpr29=arithmExpr();

                    state._fsp--;

                    stream_arithmExpr.add(arithmExpr29.getTree());

                    }
                    break;
                case 3 :
                    // Jaql.g:43:50: arrayDefinition
                    {
                    pushFollow(FOLLOW_arrayDefinition_in_jsonExpr355);
                    arrayDefinition30=arrayDefinition();

                    state._fsp--;

                    stream_arrayDefinition.add(arrayDefinition30.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: arithmExpr, arrayDefinition, ID, jsonObject
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 43:67: -> ^( JSON_FIELD ( ID )? ( jsonObject )? ( arithmExpr )? ( arrayDefinition )? )
            {
                // Jaql.g:43:70: ^( JSON_FIELD ( ID )? ( jsonObject )? ( arithmExpr )? ( arrayDefinition )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(JSON_FIELD, "JSON_FIELD"), root_1);

                // Jaql.g:43:83: ( ID )?
                if ( stream_ID.hasNext() ) {
                    adaptor.addChild(root_1, stream_ID.nextNode());

                }
                stream_ID.reset();
                // Jaql.g:43:87: ( jsonObject )?
                if ( stream_jsonObject.hasNext() ) {
                    adaptor.addChild(root_1, stream_jsonObject.nextTree());

                }
                stream_jsonObject.reset();
                // Jaql.g:43:99: ( arithmExpr )?
                if ( stream_arithmExpr.hasNext() ) {
                    adaptor.addChild(root_1, stream_arithmExpr.nextTree());

                }
                stream_arithmExpr.reset();
                // Jaql.g:43:111: ( arrayDefinition )?
                if ( stream_arrayDefinition.hasNext() ) {
                    adaptor.addChild(root_1, stream_arrayDefinition.nextTree());

                }
                stream_arrayDefinition.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "jsonExpr"

    public static class jsonObject_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "jsonObject"
    // Jaql.g:45:1: jsonObject : '{' ( jsonExpr ( ',' jsonExpr )* ( ',' )? )? '}' -> ^( JSON_OBJECT ( jsonExpr )* ) ;
    public final JaqlParser.jsonObject_return jsonObject() throws RecognitionException {
        JaqlParser.jsonObject_return retval = new JaqlParser.jsonObject_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal31=null;
        Token char_literal33=null;
        Token char_literal35=null;
        Token char_literal36=null;
        JaqlParser.jsonExpr_return jsonExpr32 = null;

        JaqlParser.jsonExpr_return jsonExpr34 = null;


        CommonTree char_literal31_tree=null;
        CommonTree char_literal33_tree=null;
        CommonTree char_literal35_tree=null;
        CommonTree char_literal36_tree=null;
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_jsonExpr=new RewriteRuleSubtreeStream(adaptor,"rule jsonExpr");
        try {
            // Jaql.g:46:2: ( '{' ( jsonExpr ( ',' jsonExpr )* ( ',' )? )? '}' -> ^( JSON_OBJECT ( jsonExpr )* ) )
            // Jaql.g:46:4: '{' ( jsonExpr ( ',' jsonExpr )* ( ',' )? )? '}'
            {
            char_literal31=(Token)match(input,46,FOLLOW_46_in_jsonObject383);  
            stream_46.add(char_literal31);

            // Jaql.g:46:8: ( jsonExpr ( ',' jsonExpr )* ( ',' )? )?
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( (LA15_0==VAR||(LA15_0>=ID && LA15_0<=UINT)||LA15_0==46||LA15_0==49) ) {
                alt15=1;
            }
            switch (alt15) {
                case 1 :
                    // Jaql.g:46:9: jsonExpr ( ',' jsonExpr )* ( ',' )?
                    {
                    pushFollow(FOLLOW_jsonExpr_in_jsonObject386);
                    jsonExpr32=jsonExpr();

                    state._fsp--;

                    stream_jsonExpr.add(jsonExpr32.getTree());
                    // Jaql.g:46:18: ( ',' jsonExpr )*
                    loop13:
                    do {
                        int alt13=2;
                        int LA13_0 = input.LA(1);

                        if ( (LA13_0==44) ) {
                            int LA13_1 = input.LA(2);

                            if ( (LA13_1==VAR||(LA13_1>=ID && LA13_1<=UINT)||LA13_1==46||LA13_1==49) ) {
                                alt13=1;
                            }


                        }


                        switch (alt13) {
                    	case 1 :
                    	    // Jaql.g:46:19: ',' jsonExpr
                    	    {
                    	    char_literal33=(Token)match(input,44,FOLLOW_44_in_jsonObject389);  
                    	    stream_44.add(char_literal33);

                    	    pushFollow(FOLLOW_jsonExpr_in_jsonObject391);
                    	    jsonExpr34=jsonExpr();

                    	    state._fsp--;

                    	    stream_jsonExpr.add(jsonExpr34.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop13;
                        }
                    } while (true);

                    // Jaql.g:46:34: ( ',' )?
                    int alt14=2;
                    int LA14_0 = input.LA(1);

                    if ( (LA14_0==44) ) {
                        alt14=1;
                    }
                    switch (alt14) {
                        case 1 :
                            // Jaql.g:46:34: ','
                            {
                            char_literal35=(Token)match(input,44,FOLLOW_44_in_jsonObject395);  
                            stream_44.add(char_literal35);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal36=(Token)match(input,47,FOLLOW_47_in_jsonObject400);  
            stream_47.add(char_literal36);



            // AST REWRITE
            // elements: jsonExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 46:45: -> ^( JSON_OBJECT ( jsonExpr )* )
            {
                // Jaql.g:46:48: ^( JSON_OBJECT ( jsonExpr )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(JSON_OBJECT, "JSON_OBJECT"), root_1);

                // Jaql.g:46:62: ( jsonExpr )*
                while ( stream_jsonExpr.hasNext() ) {
                    adaptor.addChild(root_1, stream_jsonExpr.nextTree());

                }
                stream_jsonExpr.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "jsonObject"

    public static class object_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "object"
    // Jaql.g:49:1: object : ( ID | VAR | functionCall ) ( objectExpr )? -> ^( OBJECT_EXPR ( VAR )? ( functionCall )? ( ID )? ( objectExpr )? ) ;
    public final JaqlParser.object_return object() throws RecognitionException {
        JaqlParser.object_return retval = new JaqlParser.object_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID37=null;
        Token VAR38=null;
        JaqlParser.functionCall_return functionCall39 = null;

        JaqlParser.objectExpr_return objectExpr40 = null;


        CommonTree ID37_tree=null;
        CommonTree VAR38_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_functionCall=new RewriteRuleSubtreeStream(adaptor,"rule functionCall");
        RewriteRuleSubtreeStream stream_objectExpr=new RewriteRuleSubtreeStream(adaptor,"rule objectExpr");
        try {
            // Jaql.g:49:8: ( ( ID | VAR | functionCall ) ( objectExpr )? -> ^( OBJECT_EXPR ( VAR )? ( functionCall )? ( ID )? ( objectExpr )? ) )
            // Jaql.g:49:10: ( ID | VAR | functionCall ) ( objectExpr )?
            {
            // Jaql.g:49:10: ( ID | VAR | functionCall )
            int alt16=3;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==ID) ) {
                int LA16_1 = input.LA(2);

                if ( (LA16_1==39) ) {
                    alt16=3;
                }
                else if ( (LA16_1==ARROW||(LA16_1>=STAR && LA16_1<=COMPARISON)||LA16_1==37||(LA16_1>=40 && LA16_1<=44)||(LA16_1>=47 && LA16_1<=50)||LA16_1==53||LA16_1==55||LA16_1==57||LA16_1==61) ) {
                    alt16=1;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 16, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA16_0==VAR) ) {
                alt16=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // Jaql.g:49:11: ID
                    {
                    ID37=(Token)match(input,ID,FOLLOW_ID_in_object419);  
                    stream_ID.add(ID37);


                    }
                    break;
                case 2 :
                    // Jaql.g:49:16: VAR
                    {
                    VAR38=(Token)match(input,VAR,FOLLOW_VAR_in_object423);  
                    stream_VAR.add(VAR38);


                    }
                    break;
                case 3 :
                    // Jaql.g:49:22: functionCall
                    {
                    pushFollow(FOLLOW_functionCall_in_object427);
                    functionCall39=functionCall();

                    state._fsp--;

                    stream_functionCall.add(functionCall39.getTree());

                    }
                    break;

            }

            // Jaql.g:49:36: ( objectExpr )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( ((LA17_0>=48 && LA17_0<=49)) ) {
                alt17=1;
            }
            switch (alt17) {
                case 1 :
                    // Jaql.g:49:36: objectExpr
                    {
                    pushFollow(FOLLOW_objectExpr_in_object430);
                    objectExpr40=objectExpr();

                    state._fsp--;

                    stream_objectExpr.add(objectExpr40.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: ID, functionCall, objectExpr, VAR
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 49:48: -> ^( OBJECT_EXPR ( VAR )? ( functionCall )? ( ID )? ( objectExpr )? )
            {
                // Jaql.g:49:51: ^( OBJECT_EXPR ( VAR )? ( functionCall )? ( ID )? ( objectExpr )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(OBJECT_EXPR, "OBJECT_EXPR"), root_1);

                // Jaql.g:49:65: ( VAR )?
                if ( stream_VAR.hasNext() ) {
                    adaptor.addChild(root_1, stream_VAR.nextNode());

                }
                stream_VAR.reset();
                // Jaql.g:49:70: ( functionCall )?
                if ( stream_functionCall.hasNext() ) {
                    adaptor.addChild(root_1, stream_functionCall.nextTree());

                }
                stream_functionCall.reset();
                // Jaql.g:49:84: ( ID )?
                if ( stream_ID.hasNext() ) {
                    adaptor.addChild(root_1, stream_ID.nextNode());

                }
                stream_ID.reset();
                // Jaql.g:49:88: ( objectExpr )?
                if ( stream_objectExpr.hasNext() ) {
                    adaptor.addChild(root_1, stream_objectExpr.nextTree());

                }
                stream_objectExpr.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "object"

    public static class literal_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "literal"
    // Jaql.g:51:1: literal : ( INTEGER | DECIMAL | STRING | UINT );
    public final JaqlParser.literal_return literal() throws RecognitionException {
        JaqlParser.literal_return retval = new JaqlParser.literal_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set41=null;

        CommonTree set41_tree=null;

        try {
            // Jaql.g:51:9: ( INTEGER | DECIMAL | STRING | UINT )
            // Jaql.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set41=(Token)input.LT(1);
            if ( (input.LA(1)>=INTEGER && input.LA(1)<=UINT) ) {
                input.consume();
                adaptor.addChild(root_0, (CommonTree)adaptor.create(set41));
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "literal"

    public static class objectExpr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "objectExpr"
    // Jaql.g:53:1: objectExpr : ( ( '.' ( ID | functionCall ) ) | arrayAccess ) ( objectExpr )? ;
    public final JaqlParser.objectExpr_return objectExpr() throws RecognitionException {
        JaqlParser.objectExpr_return retval = new JaqlParser.objectExpr_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal42=null;
        Token ID43=null;
        JaqlParser.functionCall_return functionCall44 = null;

        JaqlParser.arrayAccess_return arrayAccess45 = null;

        JaqlParser.objectExpr_return objectExpr46 = null;


        CommonTree char_literal42_tree=null;
        CommonTree ID43_tree=null;

        try {
            // Jaql.g:54:2: ( ( ( '.' ( ID | functionCall ) ) | arrayAccess ) ( objectExpr )? )
            // Jaql.g:54:4: ( ( '.' ( ID | functionCall ) ) | arrayAccess ) ( objectExpr )?
            {
            root_0 = (CommonTree)adaptor.nil();

            // Jaql.g:54:4: ( ( '.' ( ID | functionCall ) ) | arrayAccess )
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==48) ) {
                alt19=1;
            }
            else if ( (LA19_0==49) ) {
                alt19=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 19, 0, input);

                throw nvae;
            }
            switch (alt19) {
                case 1 :
                    // Jaql.g:54:5: ( '.' ( ID | functionCall ) )
                    {
                    // Jaql.g:54:5: ( '.' ( ID | functionCall ) )
                    // Jaql.g:54:6: '.' ( ID | functionCall )
                    {
                    char_literal42=(Token)match(input,48,FOLLOW_48_in_objectExpr480); 
                    // Jaql.g:54:11: ( ID | functionCall )
                    int alt18=2;
                    int LA18_0 = input.LA(1);

                    if ( (LA18_0==ID) ) {
                        int LA18_1 = input.LA(2);

                        if ( (LA18_1==39) ) {
                            alt18=2;
                        }
                        else if ( (LA18_1==ARROW||(LA18_1>=STAR && LA18_1<=COMPARISON)||LA18_1==37||(LA18_1>=40 && LA18_1<=44)||(LA18_1>=47 && LA18_1<=50)||LA18_1==53||LA18_1==55||LA18_1==57||LA18_1==61) ) {
                            alt18=1;
                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 18, 1, input);

                            throw nvae;
                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 18, 0, input);

                        throw nvae;
                    }
                    switch (alt18) {
                        case 1 :
                            // Jaql.g:54:12: ID
                            {
                            ID43=(Token)match(input,ID,FOLLOW_ID_in_objectExpr484); 
                            ID43_tree = (CommonTree)adaptor.create(ID43);
                            adaptor.addChild(root_0, ID43_tree);


                            }
                            break;
                        case 2 :
                            // Jaql.g:54:17: functionCall
                            {
                            pushFollow(FOLLOW_functionCall_in_objectExpr488);
                            functionCall44=functionCall();

                            state._fsp--;

                            adaptor.addChild(root_0, functionCall44.getTree());

                            }
                            break;

                    }


                    }


                    }
                    break;
                case 2 :
                    // Jaql.g:54:34: arrayAccess
                    {
                    pushFollow(FOLLOW_arrayAccess_in_objectExpr494);
                    arrayAccess45=arrayAccess();

                    state._fsp--;

                    adaptor.addChild(root_0, arrayAccess45.getTree());

                    }
                    break;

            }

            // Jaql.g:54:47: ( objectExpr )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( ((LA20_0>=48 && LA20_0<=49)) ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // Jaql.g:54:47: objectExpr
                    {
                    pushFollow(FOLLOW_objectExpr_in_objectExpr497);
                    objectExpr46=objectExpr();

                    state._fsp--;

                    adaptor.addChild(root_0, objectExpr46.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "objectExpr"

    public static class arrayAccess_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // Jaql.g:56:1: arrayAccess : '[' ( (i= INTEGER | i= UINT ) ( ':' (j= INTEGER | j= UINT ) )? | STAR ) ']' -> ^( ARRAY_ACCESS ( $i)? ( $j)? ( STAR )? ) ;
    public final JaqlParser.arrayAccess_return arrayAccess() throws RecognitionException {
        JaqlParser.arrayAccess_return retval = new JaqlParser.arrayAccess_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token i=null;
        Token j=null;
        Token char_literal47=null;
        Token char_literal48=null;
        Token STAR49=null;
        Token char_literal50=null;

        CommonTree i_tree=null;
        CommonTree j_tree=null;
        CommonTree char_literal47_tree=null;
        CommonTree char_literal48_tree=null;
        CommonTree STAR49_tree=null;
        CommonTree char_literal50_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");

        try {
            // Jaql.g:57:2: ( '[' ( (i= INTEGER | i= UINT ) ( ':' (j= INTEGER | j= UINT ) )? | STAR ) ']' -> ^( ARRAY_ACCESS ( $i)? ( $j)? ( STAR )? ) )
            // Jaql.g:57:5: '[' ( (i= INTEGER | i= UINT ) ( ':' (j= INTEGER | j= UINT ) )? | STAR ) ']'
            {
            char_literal47=(Token)match(input,49,FOLLOW_49_in_arrayAccess508);  
            stream_49.add(char_literal47);

            // Jaql.g:57:9: ( (i= INTEGER | i= UINT ) ( ':' (j= INTEGER | j= UINT ) )? | STAR )
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==INTEGER||LA24_0==UINT) ) {
                alt24=1;
            }
            else if ( (LA24_0==STAR) ) {
                alt24=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 24, 0, input);

                throw nvae;
            }
            switch (alt24) {
                case 1 :
                    // Jaql.g:57:10: (i= INTEGER | i= UINT ) ( ':' (j= INTEGER | j= UINT ) )?
                    {
                    // Jaql.g:57:10: (i= INTEGER | i= UINT )
                    int alt21=2;
                    int LA21_0 = input.LA(1);

                    if ( (LA21_0==INTEGER) ) {
                        alt21=1;
                    }
                    else if ( (LA21_0==UINT) ) {
                        alt21=2;
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 21, 0, input);

                        throw nvae;
                    }
                    switch (alt21) {
                        case 1 :
                            // Jaql.g:57:11: i= INTEGER
                            {
                            i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess514);  
                            stream_INTEGER.add(i);


                            }
                            break;
                        case 2 :
                            // Jaql.g:57:23: i= UINT
                            {
                            i=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess520);  
                            stream_UINT.add(i);


                            }
                            break;

                    }

                    // Jaql.g:57:31: ( ':' (j= INTEGER | j= UINT ) )?
                    int alt23=2;
                    int LA23_0 = input.LA(1);

                    if ( (LA23_0==45) ) {
                        alt23=1;
                    }
                    switch (alt23) {
                        case 1 :
                            // Jaql.g:57:32: ':' (j= INTEGER | j= UINT )
                            {
                            char_literal48=(Token)match(input,45,FOLLOW_45_in_arrayAccess524);  
                            stream_45.add(char_literal48);

                            // Jaql.g:57:36: (j= INTEGER | j= UINT )
                            int alt22=2;
                            int LA22_0 = input.LA(1);

                            if ( (LA22_0==INTEGER) ) {
                                alt22=1;
                            }
                            else if ( (LA22_0==UINT) ) {
                                alt22=2;
                            }
                            else {
                                NoViableAltException nvae =
                                    new NoViableAltException("", 22, 0, input);

                                throw nvae;
                            }
                            switch (alt22) {
                                case 1 :
                                    // Jaql.g:57:37: j= INTEGER
                                    {
                                    j=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess529);  
                                    stream_INTEGER.add(j);


                                    }
                                    break;
                                case 2 :
                                    // Jaql.g:57:49: j= UINT
                                    {
                                    j=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess535);  
                                    stream_UINT.add(j);


                                    }
                                    break;

                            }


                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // Jaql.g:57:61: STAR
                    {
                    STAR49=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess542);  
                    stream_STAR.add(STAR49);


                    }
                    break;

            }

            char_literal50=(Token)match(input,50,FOLLOW_50_in_arrayAccess545);  
            stream_50.add(char_literal50);



            // AST REWRITE
            // elements: i, STAR, j
            // token labels: j, i
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleTokenStream stream_j=new RewriteRuleTokenStream(adaptor,"token j",j);
            RewriteRuleTokenStream stream_i=new RewriteRuleTokenStream(adaptor,"token i",i);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 57:71: -> ^( ARRAY_ACCESS ( $i)? ( $j)? ( STAR )? )
            {
                // Jaql.g:57:74: ^( ARRAY_ACCESS ( $i)? ( $j)? ( STAR )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ARRAY_ACCESS, "ARRAY_ACCESS"), root_1);

                // Jaql.g:57:89: ( $i)?
                if ( stream_i.hasNext() ) {
                    adaptor.addChild(root_1, stream_i.nextNode());

                }
                stream_i.reset();
                // Jaql.g:57:93: ( $j)?
                if ( stream_j.hasNext() ) {
                    adaptor.addChild(root_1, stream_j.nextNode());

                }
                stream_j.reset();
                // Jaql.g:57:97: ( STAR )?
                if ( stream_STAR.hasNext() ) {
                    adaptor.addChild(root_1, stream_STAR.nextNode());

                }
                stream_STAR.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class arrayDefinition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayDefinition"
    // Jaql.g:59:1: arrayDefinition : '[' arithmExpr ( ',' arithmExpr )* ( ',' )? ']' -> ^( ARRAY ( arithmExpr )* ) ;
    public final JaqlParser.arrayDefinition_return arrayDefinition() throws RecognitionException {
        JaqlParser.arrayDefinition_return retval = new JaqlParser.arrayDefinition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal51=null;
        Token char_literal53=null;
        Token char_literal55=null;
        Token char_literal56=null;
        JaqlParser.arithmExpr_return arithmExpr52 = null;

        JaqlParser.arithmExpr_return arithmExpr54 = null;


        CommonTree char_literal51_tree=null;
        CommonTree char_literal53_tree=null;
        CommonTree char_literal55_tree=null;
        CommonTree char_literal56_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_arithmExpr=new RewriteRuleSubtreeStream(adaptor,"rule arithmExpr");
        try {
            // Jaql.g:60:2: ( '[' arithmExpr ( ',' arithmExpr )* ( ',' )? ']' -> ^( ARRAY ( arithmExpr )* ) )
            // Jaql.g:60:5: '[' arithmExpr ( ',' arithmExpr )* ( ',' )? ']'
            {
            char_literal51=(Token)match(input,49,FOLLOW_49_in_arrayDefinition573);  
            stream_49.add(char_literal51);

            pushFollow(FOLLOW_arithmExpr_in_arrayDefinition575);
            arithmExpr52=arithmExpr();

            state._fsp--;

            stream_arithmExpr.add(arithmExpr52.getTree());
            // Jaql.g:60:20: ( ',' arithmExpr )*
            loop25:
            do {
                int alt25=2;
                int LA25_0 = input.LA(1);

                if ( (LA25_0==44) ) {
                    int LA25_1 = input.LA(2);

                    if ( (LA25_1==VAR||(LA25_1>=ID && LA25_1<=UINT)) ) {
                        alt25=1;
                    }


                }


                switch (alt25) {
            	case 1 :
            	    // Jaql.g:60:21: ',' arithmExpr
            	    {
            	    char_literal53=(Token)match(input,44,FOLLOW_44_in_arrayDefinition578);  
            	    stream_44.add(char_literal53);

            	    pushFollow(FOLLOW_arithmExpr_in_arrayDefinition580);
            	    arithmExpr54=arithmExpr();

            	    state._fsp--;

            	    stream_arithmExpr.add(arithmExpr54.getTree());

            	    }
            	    break;

            	default :
            	    break loop25;
                }
            } while (true);

            // Jaql.g:60:38: ( ',' )?
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( (LA26_0==44) ) {
                alt26=1;
            }
            switch (alt26) {
                case 1 :
                    // Jaql.g:60:38: ','
                    {
                    char_literal55=(Token)match(input,44,FOLLOW_44_in_arrayDefinition584);  
                    stream_44.add(char_literal55);


                    }
                    break;

            }

            char_literal56=(Token)match(input,50,FOLLOW_50_in_arrayDefinition587);  
            stream_50.add(char_literal56);



            // AST REWRITE
            // elements: arithmExpr
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 60:47: -> ^( ARRAY ( arithmExpr )* )
            {
                // Jaql.g:60:50: ^( ARRAY ( arithmExpr )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(ARRAY, "ARRAY"), root_1);

                // Jaql.g:60:58: ( arithmExpr )*
                while ( stream_arithmExpr.hasNext() ) {
                    adaptor.addChild(root_1, stream_arithmExpr.nextTree());

                }
                stream_arithmExpr.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "arrayDefinition"

    public static class operator_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // Jaql.g:62:1: operator : ( transform | groupBy | link | fuse | substitute );
    public final JaqlParser.operator_return operator() throws RecognitionException {
        JaqlParser.operator_return retval = new JaqlParser.operator_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        JaqlParser.transform_return transform57 = null;

        JaqlParser.groupBy_return groupBy58 = null;

        JaqlParser.link_return link59 = null;

        JaqlParser.fuse_return fuse60 = null;

        JaqlParser.substitute_return substitute61 = null;



        try {
            // Jaql.g:62:10: ( transform | groupBy | link | fuse | substitute )
            int alt27=5;
            switch ( input.LA(1) ) {
            case 51:
                {
                alt27=1;
                }
                break;
            case 52:
                {
                alt27=2;
                }
                break;
            case 54:
                {
                alt27=3;
                }
                break;
            case 60:
                {
                alt27=4;
                }
                break;
            case 63:
                {
                alt27=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 27, 0, input);

                throw nvae;
            }

            switch (alt27) {
                case 1 :
                    // Jaql.g:62:12: transform
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_transform_in_operator604);
                    transform57=transform();

                    state._fsp--;

                    adaptor.addChild(root_0, transform57.getTree());

                    }
                    break;
                case 2 :
                    // Jaql.g:62:24: groupBy
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_groupBy_in_operator608);
                    groupBy58=groupBy();

                    state._fsp--;

                    adaptor.addChild(root_0, groupBy58.getTree());

                    }
                    break;
                case 3 :
                    // Jaql.g:62:34: link
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_link_in_operator612);
                    link59=link();

                    state._fsp--;

                    adaptor.addChild(root_0, link59.getTree());

                    }
                    break;
                case 4 :
                    // Jaql.g:62:41: fuse
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_fuse_in_operator616);
                    fuse60=fuse();

                    state._fsp--;

                    adaptor.addChild(root_0, fuse60.getTree());

                    }
                    break;
                case 5 :
                    // Jaql.g:62:48: substitute
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_substitute_in_operator620);
                    substitute61=substitute();

                    state._fsp--;

                    adaptor.addChild(root_0, substitute61.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "operator"

    public static class transform_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "transform"
    // Jaql.g:64:1: transform : 'transform' jsonObject -> ^( OPERATOR[\"transform\"] jsonObject ) ;
    public final JaqlParser.transform_return transform() throws RecognitionException {
        JaqlParser.transform_return retval = new JaqlParser.transform_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal62=null;
        JaqlParser.jsonObject_return jsonObject63 = null;


        CommonTree string_literal62_tree=null;
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleSubtreeStream stream_jsonObject=new RewriteRuleSubtreeStream(adaptor,"rule jsonObject");
        try {
            // Jaql.g:64:11: ( 'transform' jsonObject -> ^( OPERATOR[\"transform\"] jsonObject ) )
            // Jaql.g:64:13: 'transform' jsonObject
            {
            string_literal62=(Token)match(input,51,FOLLOW_51_in_transform629);  
            stream_51.add(string_literal62);

            pushFollow(FOLLOW_jsonObject_in_transform631);
            jsonObject63=jsonObject();

            state._fsp--;

            stream_jsonObject.add(jsonObject63.getTree());


            // AST REWRITE
            // elements: jsonObject
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 64:36: -> ^( OPERATOR[\"transform\"] jsonObject )
            {
                // Jaql.g:64:39: ^( OPERATOR[\"transform\"] jsonObject )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(OPERATOR, "transform"), root_1);

                adaptor.addChild(root_1, stream_jsonObject.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "transform"

    public static class groupBy_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "groupBy"
    // Jaql.g:66:1: groupBy : 'group by' ID ( '=' arrayDefinition )? 'into' jsonObject -> ^( OPERATOR[\"group by\"] ID ( arrayDefinition )? jsonObject ) ;
    public final JaqlParser.groupBy_return groupBy() throws RecognitionException {
        JaqlParser.groupBy_return retval = new JaqlParser.groupBy_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal64=null;
        Token ID65=null;
        Token char_literal66=null;
        Token string_literal68=null;
        JaqlParser.arrayDefinition_return arrayDefinition67 = null;

        JaqlParser.jsonObject_return jsonObject69 = null;


        CommonTree string_literal64_tree=null;
        CommonTree ID65_tree=null;
        CommonTree char_literal66_tree=null;
        CommonTree string_literal68_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_jsonObject=new RewriteRuleSubtreeStream(adaptor,"rule jsonObject");
        RewriteRuleSubtreeStream stream_arrayDefinition=new RewriteRuleSubtreeStream(adaptor,"rule arrayDefinition");
        try {
            // Jaql.g:66:9: ( 'group by' ID ( '=' arrayDefinition )? 'into' jsonObject -> ^( OPERATOR[\"group by\"] ID ( arrayDefinition )? jsonObject ) )
            // Jaql.g:66:11: 'group by' ID ( '=' arrayDefinition )? 'into' jsonObject
            {
            string_literal64=(Token)match(input,52,FOLLOW_52_in_groupBy648);  
            stream_52.add(string_literal64);

            ID65=(Token)match(input,ID,FOLLOW_ID_in_groupBy650);  
            stream_ID.add(ID65);

            // Jaql.g:66:25: ( '=' arrayDefinition )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( (LA28_0==38) ) {
                alt28=1;
            }
            switch (alt28) {
                case 1 :
                    // Jaql.g:66:26: '=' arrayDefinition
                    {
                    char_literal66=(Token)match(input,38,FOLLOW_38_in_groupBy653);  
                    stream_38.add(char_literal66);

                    pushFollow(FOLLOW_arrayDefinition_in_groupBy655);
                    arrayDefinition67=arrayDefinition();

                    state._fsp--;

                    stream_arrayDefinition.add(arrayDefinition67.getTree());

                    }
                    break;

            }

            string_literal68=(Token)match(input,53,FOLLOW_53_in_groupBy659);  
            stream_53.add(string_literal68);

            pushFollow(FOLLOW_jsonObject_in_groupBy661);
            jsonObject69=jsonObject();

            state._fsp--;

            stream_jsonObject.add(jsonObject69.getTree());


            // AST REWRITE
            // elements: ID, jsonObject, arrayDefinition
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 66:66: -> ^( OPERATOR[\"group by\"] ID ( arrayDefinition )? jsonObject )
            {
                // Jaql.g:66:69: ^( OPERATOR[\"group by\"] ID ( arrayDefinition )? jsonObject )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(OPERATOR, "group by"), root_1);

                adaptor.addChild(root_1, stream_ID.nextNode());
                // Jaql.g:66:95: ( arrayDefinition )?
                if ( stream_arrayDefinition.hasNext() ) {
                    adaptor.addChild(root_1, stream_arrayDefinition.nextTree());

                }
                stream_arrayDefinition.reset();
                adaptor.addChild(root_1, stream_jsonObject.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "groupBy"

    public static class link_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "link"
    // Jaql.g:68:1: link : 'link' bindings 'where' linkCondition ( logicOperator linkCondition )* ( 'partition' 'with' ID 'on' arrayDefinition )? 'into' jsonObject -> ^( OPERATOR[\"link\"] bindings ( linkCondition )* ID arrayDefinition jsonObject ) ;
    public final JaqlParser.link_return link() throws RecognitionException {
        JaqlParser.link_return retval = new JaqlParser.link_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal70=null;
        Token string_literal72=null;
        Token string_literal76=null;
        Token string_literal77=null;
        Token ID78=null;
        Token string_literal79=null;
        Token string_literal81=null;
        JaqlParser.bindings_return bindings71 = null;

        JaqlParser.linkCondition_return linkCondition73 = null;

        JaqlParser.logicOperator_return logicOperator74 = null;

        JaqlParser.linkCondition_return linkCondition75 = null;

        JaqlParser.arrayDefinition_return arrayDefinition80 = null;

        JaqlParser.jsonObject_return jsonObject82 = null;


        CommonTree string_literal70_tree=null;
        CommonTree string_literal72_tree=null;
        CommonTree string_literal76_tree=null;
        CommonTree string_literal77_tree=null;
        CommonTree ID78_tree=null;
        CommonTree string_literal79_tree=null;
        CommonTree string_literal81_tree=null;
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_54=new RewriteRuleTokenStream(adaptor,"token 54");
        RewriteRuleSubtreeStream stream_logicOperator=new RewriteRuleSubtreeStream(adaptor,"rule logicOperator");
        RewriteRuleSubtreeStream stream_jsonObject=new RewriteRuleSubtreeStream(adaptor,"rule jsonObject");
        RewriteRuleSubtreeStream stream_arrayDefinition=new RewriteRuleSubtreeStream(adaptor,"rule arrayDefinition");
        RewriteRuleSubtreeStream stream_bindings=new RewriteRuleSubtreeStream(adaptor,"rule bindings");
        RewriteRuleSubtreeStream stream_linkCondition=new RewriteRuleSubtreeStream(adaptor,"rule linkCondition");
        try {
            // Jaql.g:68:6: ( 'link' bindings 'where' linkCondition ( logicOperator linkCondition )* ( 'partition' 'with' ID 'on' arrayDefinition )? 'into' jsonObject -> ^( OPERATOR[\"link\"] bindings ( linkCondition )* ID arrayDefinition jsonObject ) )
            // Jaql.g:68:9: 'link' bindings 'where' linkCondition ( logicOperator linkCondition )* ( 'partition' 'with' ID 'on' arrayDefinition )? 'into' jsonObject
            {
            string_literal70=(Token)match(input,54,FOLLOW_54_in_link684);  
            stream_54.add(string_literal70);

            pushFollow(FOLLOW_bindings_in_link686);
            bindings71=bindings();

            state._fsp--;

            stream_bindings.add(bindings71.getTree());
            string_literal72=(Token)match(input,55,FOLLOW_55_in_link688);  
            stream_55.add(string_literal72);

            pushFollow(FOLLOW_linkCondition_in_link690);
            linkCondition73=linkCondition();

            state._fsp--;

            stream_linkCondition.add(linkCondition73.getTree());
            // Jaql.g:68:47: ( logicOperator linkCondition )*
            loop29:
            do {
                int alt29=2;
                int LA29_0 = input.LA(1);

                if ( ((LA29_0>=64 && LA29_0<=66)) ) {
                    alt29=1;
                }


                switch (alt29) {
            	case 1 :
            	    // Jaql.g:68:48: logicOperator linkCondition
            	    {
            	    pushFollow(FOLLOW_logicOperator_in_link693);
            	    logicOperator74=logicOperator();

            	    state._fsp--;

            	    stream_logicOperator.add(logicOperator74.getTree());
            	    pushFollow(FOLLOW_linkCondition_in_link695);
            	    linkCondition75=linkCondition();

            	    state._fsp--;

            	    stream_linkCondition.add(linkCondition75.getTree());

            	    }
            	    break;

            	default :
            	    break loop29;
                }
            } while (true);

            // Jaql.g:68:78: ( 'partition' 'with' ID 'on' arrayDefinition )?
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==56) ) {
                alt30=1;
            }
            switch (alt30) {
                case 1 :
                    // Jaql.g:68:79: 'partition' 'with' ID 'on' arrayDefinition
                    {
                    string_literal76=(Token)match(input,56,FOLLOW_56_in_link700);  
                    stream_56.add(string_literal76);

                    string_literal77=(Token)match(input,57,FOLLOW_57_in_link702);  
                    stream_57.add(string_literal77);

                    ID78=(Token)match(input,ID,FOLLOW_ID_in_link704);  
                    stream_ID.add(ID78);

                    string_literal79=(Token)match(input,58,FOLLOW_58_in_link706);  
                    stream_58.add(string_literal79);

                    pushFollow(FOLLOW_arrayDefinition_in_link708);
                    arrayDefinition80=arrayDefinition();

                    state._fsp--;

                    stream_arrayDefinition.add(arrayDefinition80.getTree());

                    }
                    break;

            }

            string_literal81=(Token)match(input,53,FOLLOW_53_in_link712);  
            stream_53.add(string_literal81);

            pushFollow(FOLLOW_jsonObject_in_link714);
            jsonObject82=jsonObject();

            state._fsp--;

            stream_jsonObject.add(jsonObject82.getTree());


            // AST REWRITE
            // elements: linkCondition, ID, arrayDefinition, jsonObject, bindings
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 69:3: -> ^( OPERATOR[\"link\"] bindings ( linkCondition )* ID arrayDefinition jsonObject )
            {
                // Jaql.g:69:6: ^( OPERATOR[\"link\"] bindings ( linkCondition )* ID arrayDefinition jsonObject )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(OPERATOR, "link"), root_1);

                adaptor.addChild(root_1, stream_bindings.nextTree());
                // Jaql.g:69:34: ( linkCondition )*
                while ( stream_linkCondition.hasNext() ) {
                    adaptor.addChild(root_1, stream_linkCondition.nextTree());

                }
                stream_linkCondition.reset();
                adaptor.addChild(root_1, stream_ID.nextNode());
                adaptor.addChild(root_1, stream_arrayDefinition.nextTree());
                adaptor.addChild(root_1, stream_jsonObject.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "link"

    public static class linkCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "linkCondition"
    // Jaql.g:71:1: linkCondition : ( arithmExpr | arrayDefinition ) COMPARISON ( INTEGER | DECIMAL ) ;
    public final JaqlParser.linkCondition_return linkCondition() throws RecognitionException {
        JaqlParser.linkCondition_return retval = new JaqlParser.linkCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token COMPARISON85=null;
        Token set86=null;
        JaqlParser.arithmExpr_return arithmExpr83 = null;

        JaqlParser.arrayDefinition_return arrayDefinition84 = null;


        CommonTree COMPARISON85_tree=null;
        CommonTree set86_tree=null;

        try {
            // Jaql.g:72:2: ( ( arithmExpr | arrayDefinition ) COMPARISON ( INTEGER | DECIMAL ) )
            // Jaql.g:72:4: ( arithmExpr | arrayDefinition ) COMPARISON ( INTEGER | DECIMAL )
            {
            root_0 = (CommonTree)adaptor.nil();

            // Jaql.g:72:4: ( arithmExpr | arrayDefinition )
            int alt31=2;
            int LA31_0 = input.LA(1);

            if ( (LA31_0==VAR||(LA31_0>=ID && LA31_0<=UINT)) ) {
                alt31=1;
            }
            else if ( (LA31_0==49) ) {
                alt31=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;
            }
            switch (alt31) {
                case 1 :
                    // Jaql.g:72:5: arithmExpr
                    {
                    pushFollow(FOLLOW_arithmExpr_in_linkCondition745);
                    arithmExpr83=arithmExpr();

                    state._fsp--;

                    adaptor.addChild(root_0, arithmExpr83.getTree());

                    }
                    break;
                case 2 :
                    // Jaql.g:72:18: arrayDefinition
                    {
                    pushFollow(FOLLOW_arrayDefinition_in_linkCondition749);
                    arrayDefinition84=arrayDefinition();

                    state._fsp--;

                    adaptor.addChild(root_0, arrayDefinition84.getTree());

                    }
                    break;

            }

            COMPARISON85=(Token)match(input,COMPARISON,FOLLOW_COMPARISON_in_linkCondition752); 
            COMPARISON85_tree = (CommonTree)adaptor.create(COMPARISON85);
            adaptor.addChild(root_0, COMPARISON85_tree);

            set86=(Token)input.LT(1);
            if ( (input.LA(1)>=INTEGER && input.LA(1)<=DECIMAL) ) {
                input.consume();
                adaptor.addChild(root_0, (CommonTree)adaptor.create(set86));
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "linkCondition"

    public static class bindings_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "bindings"
    // Jaql.g:74:1: bindings : binding ( ',' binding )* ;
    public final JaqlParser.bindings_return bindings() throws RecognitionException {
        JaqlParser.bindings_return retval = new JaqlParser.bindings_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token char_literal88=null;
        JaqlParser.binding_return binding87 = null;

        JaqlParser.binding_return binding89 = null;


        CommonTree char_literal88_tree=null;

        try {
            // Jaql.g:74:9: ( binding ( ',' binding )* )
            // Jaql.g:74:11: binding ( ',' binding )*
            {
            root_0 = (CommonTree)adaptor.nil();

            pushFollow(FOLLOW_binding_in_bindings767);
            binding87=binding();

            state._fsp--;

            adaptor.addChild(root_0, binding87.getTree());
            // Jaql.g:74:19: ( ',' binding )*
            loop32:
            do {
                int alt32=2;
                int LA32_0 = input.LA(1);

                if ( (LA32_0==44) ) {
                    alt32=1;
                }


                switch (alt32) {
            	case 1 :
            	    // Jaql.g:74:20: ',' binding
            	    {
            	    char_literal88=(Token)match(input,44,FOLLOW_44_in_bindings770); 
            	    pushFollow(FOLLOW_binding_in_bindings773);
            	    binding89=binding();

            	    state._fsp--;

            	    adaptor.addChild(root_0, binding89.getTree());

            	    }
            	    break;

            	default :
            	    break loop32;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "bindings"

    public static class binding_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "binding"
    // Jaql.g:76:1: binding : ID 'in' object -> ^( BIND ID object ) ;
    public final JaqlParser.binding_return binding() throws RecognitionException {
        JaqlParser.binding_return retval = new JaqlParser.binding_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token ID90=null;
        Token string_literal91=null;
        JaqlParser.object_return object92 = null;


        CommonTree ID90_tree=null;
        CommonTree string_literal91_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_object=new RewriteRuleSubtreeStream(adaptor,"rule object");
        try {
            // Jaql.g:77:2: ( ID 'in' object -> ^( BIND ID object ) )
            // Jaql.g:77:5: ID 'in' object
            {
            ID90=(Token)match(input,ID,FOLLOW_ID_in_binding785);  
            stream_ID.add(ID90);

            string_literal91=(Token)match(input,59,FOLLOW_59_in_binding787);  
            stream_59.add(string_literal91);

            pushFollow(FOLLOW_object_in_binding789);
            object92=object();

            state._fsp--;

            stream_object.add(object92.getTree());


            // AST REWRITE
            // elements: object, ID
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 77:20: -> ^( BIND ID object )
            {
                // Jaql.g:77:23: ^( BIND ID object )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(BIND, "BIND"), root_1);

                adaptor.addChild(root_1, stream_ID.nextNode());
                adaptor.addChild(root_1, stream_object.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "binding"

    public static class fuse_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fuse"
    // Jaql.g:79:1: fuse : 'fuse' bindings 'where' bindingCondition 'into' jsonObject ( 'using' 'weights' jsonObject )? -> ^( OPERATOR[\"fuse\"] bindings bindingCondition jsonObject ( jsonObject )? ) ;
    public final JaqlParser.fuse_return fuse() throws RecognitionException {
        JaqlParser.fuse_return retval = new JaqlParser.fuse_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal93=null;
        Token string_literal95=null;
        Token string_literal97=null;
        Token string_literal99=null;
        Token string_literal100=null;
        JaqlParser.bindings_return bindings94 = null;

        JaqlParser.bindingCondition_return bindingCondition96 = null;

        JaqlParser.jsonObject_return jsonObject98 = null;

        JaqlParser.jsonObject_return jsonObject101 = null;


        CommonTree string_literal93_tree=null;
        CommonTree string_literal95_tree=null;
        CommonTree string_literal97_tree=null;
        CommonTree string_literal99_tree=null;
        CommonTree string_literal100_tree=null;
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_62=new RewriteRuleTokenStream(adaptor,"token 62");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");
        RewriteRuleSubtreeStream stream_jsonObject=new RewriteRuleSubtreeStream(adaptor,"rule jsonObject");
        RewriteRuleSubtreeStream stream_bindings=new RewriteRuleSubtreeStream(adaptor,"rule bindings");
        RewriteRuleSubtreeStream stream_bindingCondition=new RewriteRuleSubtreeStream(adaptor,"rule bindingCondition");
        try {
            // Jaql.g:79:6: ( 'fuse' bindings 'where' bindingCondition 'into' jsonObject ( 'using' 'weights' jsonObject )? -> ^( OPERATOR[\"fuse\"] bindings bindingCondition jsonObject ( jsonObject )? ) )
            // Jaql.g:79:9: 'fuse' bindings 'where' bindingCondition 'into' jsonObject ( 'using' 'weights' jsonObject )?
            {
            string_literal93=(Token)match(input,60,FOLLOW_60_in_fuse809);  
            stream_60.add(string_literal93);

            pushFollow(FOLLOW_bindings_in_fuse811);
            bindings94=bindings();

            state._fsp--;

            stream_bindings.add(bindings94.getTree());
            string_literal95=(Token)match(input,55,FOLLOW_55_in_fuse813);  
            stream_55.add(string_literal95);

            pushFollow(FOLLOW_bindingCondition_in_fuse815);
            bindingCondition96=bindingCondition();

            state._fsp--;

            stream_bindingCondition.add(bindingCondition96.getTree());
            string_literal97=(Token)match(input,53,FOLLOW_53_in_fuse817);  
            stream_53.add(string_literal97);

            pushFollow(FOLLOW_jsonObject_in_fuse819);
            jsonObject98=jsonObject();

            state._fsp--;

            stream_jsonObject.add(jsonObject98.getTree());
            // Jaql.g:79:68: ( 'using' 'weights' jsonObject )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==61) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // Jaql.g:79:69: 'using' 'weights' jsonObject
                    {
                    string_literal99=(Token)match(input,61,FOLLOW_61_in_fuse822);  
                    stream_61.add(string_literal99);

                    string_literal100=(Token)match(input,62,FOLLOW_62_in_fuse824);  
                    stream_62.add(string_literal100);

                    pushFollow(FOLLOW_jsonObject_in_fuse826);
                    jsonObject101=jsonObject();

                    state._fsp--;

                    stream_jsonObject.add(jsonObject101.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: bindings, jsonObject, jsonObject, bindingCondition
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 80:3: -> ^( OPERATOR[\"fuse\"] bindings bindingCondition jsonObject ( jsonObject )? )
            {
                // Jaql.g:80:6: ^( OPERATOR[\"fuse\"] bindings bindingCondition jsonObject ( jsonObject )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(OPERATOR, "fuse"), root_1);

                adaptor.addChild(root_1, stream_bindings.nextTree());
                adaptor.addChild(root_1, stream_bindingCondition.nextTree());
                adaptor.addChild(root_1, stream_jsonObject.nextTree());
                // Jaql.g:80:62: ( jsonObject )?
                if ( stream_jsonObject.hasNext() ) {
                    adaptor.addChild(root_1, stream_jsonObject.nextTree());

                }
                stream_jsonObject.reset();

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;
            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "fuse"

    public static class bindingCondition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "bindingCondition"
    // Jaql.g:82:1: bindingCondition : ( binding | ( jsonObject 'in' object ) );
    public final JaqlParser.bindingCondition_return bindingCondition() throws RecognitionException {
        JaqlParser.bindingCondition_return retval = new JaqlParser.bindingCondition_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal104=null;
        JaqlParser.binding_return binding102 = null;

        JaqlParser.jsonObject_return jsonObject103 = null;

        JaqlParser.object_return object105 = null;


        CommonTree string_literal104_tree=null;

        try {
            // Jaql.g:83:2: ( binding | ( jsonObject 'in' object ) )
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( (LA34_0==ID) ) {
                alt34=1;
            }
            else if ( (LA34_0==46) ) {
                alt34=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 34, 0, input);

                throw nvae;
            }
            switch (alt34) {
                case 1 :
                    // Jaql.g:83:4: binding
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    pushFollow(FOLLOW_binding_in_bindingCondition856);
                    binding102=binding();

                    state._fsp--;

                    adaptor.addChild(root_0, binding102.getTree());

                    }
                    break;
                case 2 :
                    // Jaql.g:83:14: ( jsonObject 'in' object )
                    {
                    root_0 = (CommonTree)adaptor.nil();

                    // Jaql.g:83:14: ( jsonObject 'in' object )
                    // Jaql.g:83:15: jsonObject 'in' object
                    {
                    pushFollow(FOLLOW_jsonObject_in_bindingCondition861);
                    jsonObject103=jsonObject();

                    state._fsp--;

                    adaptor.addChild(root_0, jsonObject103.getTree());
                    string_literal104=(Token)match(input,59,FOLLOW_59_in_bindingCondition863); 
                    string_literal104_tree = (CommonTree)adaptor.create(string_literal104);
                    adaptor.addChild(root_0, string_literal104_tree);

                    pushFollow(FOLLOW_object_in_bindingCondition865);
                    object105=object();

                    state._fsp--;

                    adaptor.addChild(root_0, object105.getTree());

                    }


                    }
                    break;

            }
            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "bindingCondition"

    public static class substitute_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "substitute"
    // Jaql.g:85:1: substitute : 'substitute' binding 'using' bindings 'where' bindingCondition 'with' object ;
    public final JaqlParser.substitute_return substitute() throws RecognitionException {
        JaqlParser.substitute_return retval = new JaqlParser.substitute_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token string_literal106=null;
        Token string_literal108=null;
        Token string_literal110=null;
        Token string_literal112=null;
        JaqlParser.binding_return binding107 = null;

        JaqlParser.bindings_return bindings109 = null;

        JaqlParser.bindingCondition_return bindingCondition111 = null;

        JaqlParser.object_return object113 = null;


        CommonTree string_literal106_tree=null;
        CommonTree string_literal108_tree=null;
        CommonTree string_literal110_tree=null;
        CommonTree string_literal112_tree=null;

        try {
            // Jaql.g:86:2: ( 'substitute' binding 'using' bindings 'where' bindingCondition 'with' object )
            // Jaql.g:86:4: 'substitute' binding 'using' bindings 'where' bindingCondition 'with' object
            {
            root_0 = (CommonTree)adaptor.nil();

            string_literal106=(Token)match(input,63,FOLLOW_63_in_substitute876); 
            string_literal106_tree = (CommonTree)adaptor.create(string_literal106);
            adaptor.addChild(root_0, string_literal106_tree);

            pushFollow(FOLLOW_binding_in_substitute878);
            binding107=binding();

            state._fsp--;

            adaptor.addChild(root_0, binding107.getTree());
            string_literal108=(Token)match(input,61,FOLLOW_61_in_substitute880); 
            string_literal108_tree = (CommonTree)adaptor.create(string_literal108);
            adaptor.addChild(root_0, string_literal108_tree);

            pushFollow(FOLLOW_bindings_in_substitute882);
            bindings109=bindings();

            state._fsp--;

            adaptor.addChild(root_0, bindings109.getTree());
            string_literal110=(Token)match(input,55,FOLLOW_55_in_substitute884); 
            string_literal110_tree = (CommonTree)adaptor.create(string_literal110);
            adaptor.addChild(root_0, string_literal110_tree);

            pushFollow(FOLLOW_bindingCondition_in_substitute886);
            bindingCondition111=bindingCondition();

            state._fsp--;

            adaptor.addChild(root_0, bindingCondition111.getTree());
            string_literal112=(Token)match(input,57,FOLLOW_57_in_substitute888); 
            string_literal112_tree = (CommonTree)adaptor.create(string_literal112);
            adaptor.addChild(root_0, string_literal112_tree);

            pushFollow(FOLLOW_object_in_substitute890);
            object113=object();

            state._fsp--;

            adaptor.addChild(root_0, object113.getTree());

            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "substitute"

    public static class logicOperator_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "logicOperator"
    // Jaql.g:88:1: logicOperator : ( 'and' | 'or' | 'not' );
    public final JaqlParser.logicOperator_return logicOperator() throws RecognitionException {
        JaqlParser.logicOperator_return retval = new JaqlParser.logicOperator_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        Token set114=null;

        CommonTree set114_tree=null;

        try {
            // Jaql.g:89:2: ( 'and' | 'or' | 'not' )
            // Jaql.g:
            {
            root_0 = (CommonTree)adaptor.nil();

            set114=(Token)input.LT(1);
            if ( (input.LA(1)>=64 && input.LA(1)<=66) ) {
                input.consume();
                adaptor.addChild(root_0, (CommonTree)adaptor.create(set114));
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "logicOperator"

    // Delegated rules


 

    public static final BitSet FOLLOW_statement_in_script139 = new BitSet(new long[]{0x90580000000A0002L});
    public static final BitSet FOLLOW_assignment_in_statement158 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_streamStart_in_statement162 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_37_in_statement165 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_expr177 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_expr181 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_streamStart190 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_expr_in_streamStart194 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_stream_in_streamStart199 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ARROW_in_stream224 = new BitSet(new long[]{0x90580000000E0000L});
    public static final BitSet FOLLOW_expr_in_stream227 = new BitSet(new long[]{0x0000000000040002L});
    public static final BitSet FOLLOW_stream_in_stream229 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_assignment238 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_38_in_assignment240 = new BitSet(new long[]{0x90580000000A0000L});
    public static final BitSet FOLLOW_streamStart_in_assignment242 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionCall260 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_39_in_functionCall262 = new BitSet(new long[]{0x0000010000FA0000L});
    public static final BitSet FOLLOW_params_in_functionCall264 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_40_in_functionCall267 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_object_in_arithmExpr287 = new BitSet(new long[]{0x00000E0001000002L});
    public static final BitSet FOLLOW_literal_in_arithmExpr291 = new BitSet(new long[]{0x00000E0001000002L});
    public static final BitSet FOLLOW_set_in_arithmExpr295 = new BitSet(new long[]{0x0000000000FA0000L});
    public static final BitSet FOLLOW_arithmExpr_in_arithmExpr311 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmExpr_in_params322 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_44_in_params325 = new BitSet(new long[]{0x0000000000FA0000L});
    public static final BitSet FOLLOW_arithmExpr_in_params328 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_ID_in_jsonExpr340 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_45_in_jsonExpr342 = new BitSet(new long[]{0x0002400000FA0000L});
    public static final BitSet FOLLOW_jsonObject_in_jsonExpr347 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmExpr_in_jsonExpr351 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayDefinition_in_jsonExpr355 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_46_in_jsonObject383 = new BitSet(new long[]{0x0002C00000FA0000L});
    public static final BitSet FOLLOW_jsonExpr_in_jsonObject386 = new BitSet(new long[]{0x0000900000000000L});
    public static final BitSet FOLLOW_44_in_jsonObject389 = new BitSet(new long[]{0x0002400000FA0000L});
    public static final BitSet FOLLOW_jsonExpr_in_jsonObject391 = new BitSet(new long[]{0x0000900000000000L});
    public static final BitSet FOLLOW_44_in_jsonObject395 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_jsonObject400 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_object419 = new BitSet(new long[]{0x0003000000000002L});
    public static final BitSet FOLLOW_VAR_in_object423 = new BitSet(new long[]{0x0003000000000002L});
    public static final BitSet FOLLOW_functionCall_in_object427 = new BitSet(new long[]{0x0003000000000002L});
    public static final BitSet FOLLOW_objectExpr_in_object430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_literal0 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_objectExpr480 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_ID_in_objectExpr484 = new BitSet(new long[]{0x0003000000000002L});
    public static final BitSet FOLLOW_functionCall_in_objectExpr488 = new BitSet(new long[]{0x0003000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_objectExpr494 = new BitSet(new long[]{0x0003000000000002L});
    public static final BitSet FOLLOW_objectExpr_in_objectExpr497 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_49_in_arrayAccess508 = new BitSet(new long[]{0x0000000001900000L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess514 = new BitSet(new long[]{0x0004200000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess520 = new BitSet(new long[]{0x0004200000000000L});
    public static final BitSet FOLLOW_45_in_arrayAccess524 = new BitSet(new long[]{0x0000000000900000L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess529 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess535 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess542 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_arrayAccess545 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_49_in_arrayDefinition573 = new BitSet(new long[]{0x0000000000FA0000L});
    public static final BitSet FOLLOW_arithmExpr_in_arrayDefinition575 = new BitSet(new long[]{0x0004100000000000L});
    public static final BitSet FOLLOW_44_in_arrayDefinition578 = new BitSet(new long[]{0x0000000000FA0000L});
    public static final BitSet FOLLOW_arithmExpr_in_arrayDefinition580 = new BitSet(new long[]{0x0004100000000000L});
    public static final BitSet FOLLOW_44_in_arrayDefinition584 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_arrayDefinition587 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_transform_in_operator604 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_groupBy_in_operator608 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_link_in_operator612 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_fuse_in_operator616 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_substitute_in_operator620 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_51_in_transform629 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_jsonObject_in_transform631 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_52_in_groupBy648 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_ID_in_groupBy650 = new BitSet(new long[]{0x0020004000000000L});
    public static final BitSet FOLLOW_38_in_groupBy653 = new BitSet(new long[]{0x0002400000FA0000L});
    public static final BitSet FOLLOW_arrayDefinition_in_groupBy655 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_groupBy659 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_jsonObject_in_groupBy661 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_54_in_link684 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_bindings_in_link686 = new BitSet(new long[]{0x0080000000000000L});
    public static final BitSet FOLLOW_55_in_link688 = new BitSet(new long[]{0x0002400000FA0000L});
    public static final BitSet FOLLOW_linkCondition_in_link690 = new BitSet(new long[]{0x0120000000000000L,0x0000000000000007L});
    public static final BitSet FOLLOW_logicOperator_in_link693 = new BitSet(new long[]{0x0002400000FA0000L});
    public static final BitSet FOLLOW_linkCondition_in_link695 = new BitSet(new long[]{0x0120000000000000L,0x0000000000000007L});
    public static final BitSet FOLLOW_56_in_link700 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_link702 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_ID_in_link704 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_58_in_link706 = new BitSet(new long[]{0x0002400000FA0000L});
    public static final BitSet FOLLOW_arrayDefinition_in_link708 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_link712 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_jsonObject_in_link714 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmExpr_in_linkCondition745 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_arrayDefinition_in_linkCondition749 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_COMPARISON_in_linkCondition752 = new BitSet(new long[]{0x0000000000300000L});
    public static final BitSet FOLLOW_set_in_linkCondition754 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_binding_in_bindings767 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_44_in_bindings770 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_binding_in_bindings773 = new BitSet(new long[]{0x0000100000000002L});
    public static final BitSet FOLLOW_ID_in_binding785 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_binding787 = new BitSet(new long[]{0x00000000000A0000L});
    public static final BitSet FOLLOW_object_in_binding789 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_60_in_fuse809 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_bindings_in_fuse811 = new BitSet(new long[]{0x0080000000000000L});
    public static final BitSet FOLLOW_55_in_fuse813 = new BitSet(new long[]{0x0000400000080000L});
    public static final BitSet FOLLOW_bindingCondition_in_fuse815 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_fuse817 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_jsonObject_in_fuse819 = new BitSet(new long[]{0x2000000000000002L});
    public static final BitSet FOLLOW_61_in_fuse822 = new BitSet(new long[]{0x4000000000000000L});
    public static final BitSet FOLLOW_62_in_fuse824 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_jsonObject_in_fuse826 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_binding_in_bindingCondition856 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_jsonObject_in_bindingCondition861 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_bindingCondition863 = new BitSet(new long[]{0x00000000000A0000L});
    public static final BitSet FOLLOW_object_in_bindingCondition865 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_substitute876 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_binding_in_substitute878 = new BitSet(new long[]{0x2000000000000000L});
    public static final BitSet FOLLOW_61_in_substitute880 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_bindings_in_substitute882 = new BitSet(new long[]{0x0080000000000000L});
    public static final BitSet FOLLOW_55_in_substitute884 = new BitSet(new long[]{0x0000400000080000L});
    public static final BitSet FOLLOW_bindingCondition_in_substitute886 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_substitute888 = new BitSet(new long[]{0x00000000000A0000L});
    public static final BitSet FOLLOW_object_in_substitute890 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_logicOperator0 = new BitSet(new long[]{0x0000000000000002L});

}