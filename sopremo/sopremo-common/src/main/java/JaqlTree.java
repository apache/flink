// $ANTLR 3.3 Nov 30, 2010 12:50:56 JaqlTree.g 2011-02-13 16:29:16

import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class JaqlTree extends TreeRewriter {
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


        public JaqlTree(TreeNodeStream input) {
            this(input, new RecognizerSharedState());
        }
        public JaqlTree(TreeNodeStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return JaqlTree.tokenNames; }
    public String getGrammarFileName() { return "JaqlTree.g"; }


    public static class topdown_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "topdown"
    // JaqlTree.g:10:1: topdown : ( fieldAccess | methodCalls | streaming | arrayAccess );
    public final JaqlTree.topdown_return topdown() throws RecognitionException {
        JaqlTree.topdown_return retval = new JaqlTree.topdown_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        JaqlTree.fieldAccess_return fieldAccess1 = null;

        JaqlTree.methodCalls_return methodCalls2 = null;

        JaqlTree.streaming_return streaming3 = null;

        JaqlTree.arrayAccess_return arrayAccess4 = null;



        try {
            // JaqlTree.g:10:9: ( fieldAccess | methodCalls | streaming | arrayAccess )
            int alt1=4;
            alt1 = dfa1.predict(input);
            switch (alt1) {
                case 1 :
                    // JaqlTree.g:10:11: fieldAccess
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_fieldAccess_in_topdown53);
                    fieldAccess1=fieldAccess();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==1 ) 
                     
                    if ( _first_0==null ) _first_0 = fieldAccess1.tree;

                    if ( state.backtracking==1 ) {
                    retval.tree = (CommonTree)_first_0;
                    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    }
                    break;
                case 2 :
                    // JaqlTree.g:10:25: methodCalls
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_methodCalls_in_topdown57);
                    methodCalls2=methodCalls();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==1 ) 
                     
                    if ( _first_0==null ) _first_0 = methodCalls2.tree;

                    if ( state.backtracking==1 ) {
                    retval.tree = (CommonTree)_first_0;
                    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    }
                    break;
                case 3 :
                    // JaqlTree.g:10:39: streaming
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_streaming_in_topdown61);
                    streaming3=streaming();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==1 ) 
                     
                    if ( _first_0==null ) _first_0 = streaming3.tree;

                    if ( state.backtracking==1 ) {
                    retval.tree = (CommonTree)_first_0;
                    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    }
                    break;
                case 4 :
                    // JaqlTree.g:10:51: arrayAccess
                    {
                    _last = (CommonTree)input.LT(1);
                    pushFollow(FOLLOW_arrayAccess_in_topdown65);
                    arrayAccess4=arrayAccess();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==1 ) 
                     
                    if ( _first_0==null ) _first_0 = arrayAccess4.tree;

                    if ( state.backtracking==1 ) {
                    retval.tree = (CommonTree)_first_0;
                    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "topdown"

    public static class fieldAccess_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fieldAccess"
    // JaqlTree.g:12:1: fieldAccess : ( ^( OBJECT_EXPR VAR (accesses= . )+ ID ) -> ^( FIELD ^( OBJECT_EXPR VAR $accesses) ID ) | ^( OBJECT_EXPR VAR ID ) -> ^( FIELD VAR ID ) );
    public final JaqlTree.fieldAccess_return fieldAccess() throws RecognitionException {
        JaqlTree.fieldAccess_return retval = new JaqlTree.fieldAccess_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree OBJECT_EXPR5=null;
        CommonTree VAR6=null;
        CommonTree ID7=null;
        CommonTree OBJECT_EXPR8=null;
        CommonTree VAR9=null;
        CommonTree ID10=null;
        CommonTree accesses=null;

        CommonTree OBJECT_EXPR5_tree=null;
        CommonTree VAR6_tree=null;
        CommonTree ID7_tree=null;
        CommonTree OBJECT_EXPR8_tree=null;
        CommonTree VAR9_tree=null;
        CommonTree ID10_tree=null;
        CommonTree accesses_tree=null;
        RewriteRuleNodeStream stream_VAR=new RewriteRuleNodeStream(adaptor,"token VAR");
        RewriteRuleNodeStream stream_OBJECT_EXPR=new RewriteRuleNodeStream(adaptor,"token OBJECT_EXPR");
        RewriteRuleNodeStream stream_ID=new RewriteRuleNodeStream(adaptor,"token ID");

        try {
            // JaqlTree.g:12:13: ( ^( OBJECT_EXPR VAR (accesses= . )+ ID ) -> ^( FIELD ^( OBJECT_EXPR VAR $accesses) ID ) | ^( OBJECT_EXPR VAR ID ) -> ^( FIELD VAR ID ) )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==OBJECT_EXPR) ) {
                int LA3_1 = input.LA(2);

                if ( (LA3_1==DOWN) ) {
                    int LA3_2 = input.LA(3);

                    if ( (LA3_2==VAR) ) {
                        int LA3_3 = input.LA(4);

                        if ( (LA3_3==ID) ) {
                            int LA3_4 = input.LA(5);

                            if ( (LA3_4==UP) ) {
                                alt3=2;
                            }
                            else if ( (LA3_4==DOWN||(LA3_4>=SCRIPT && LA3_4<=66)) ) {
                                alt3=1;
                            }
                            else {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                NoViableAltException nvae =
                                    new NoViableAltException("", 3, 4, input);

                                throw nvae;
                            }
                        }
                        else if ( ((LA3_3>=SCRIPT && LA3_3<=ARROW)||(LA3_3>=INTEGER && LA3_3<=66)) ) {
                            alt3=1;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 3, 3, input);

                            throw nvae;
                        }
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 3, 2, input);

                        throw nvae;
                    }
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 3, 1, input);

                    throw nvae;
                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // JaqlTree.g:12:15: ^( OBJECT_EXPR VAR (accesses= . )+ ID )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    OBJECT_EXPR5=(CommonTree)match(input,OBJECT_EXPR,FOLLOW_OBJECT_EXPR_in_fieldAccess74); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OBJECT_EXPR.add(OBJECT_EXPR5);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = OBJECT_EXPR5;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    VAR6=(CommonTree)match(input,VAR,FOLLOW_VAR_in_fieldAccess76); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_VAR.add(VAR6);

                    // JaqlTree.g:12:41: (accesses= . )+
                    int cnt2=0;
                    loop2:
                    do {
                        int alt2=2;
                        int LA2_0 = input.LA(1);

                        if ( (LA2_0==ID) ) {
                            int LA2_1 = input.LA(2);

                            if ( (LA2_1==DOWN||(LA2_1>=SCRIPT && LA2_1<=66)) ) {
                                alt2=1;
                            }


                        }
                        else if ( ((LA2_0>=SCRIPT && LA2_0<=ARROW)||(LA2_0>=INTEGER && LA2_0<=66)) ) {
                            alt2=1;
                        }


                        switch (alt2) {
                    	case 1 :
                    	    // JaqlTree.g:12:41: accesses= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    accesses=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_1==null ) _first_1 = accesses;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    if ( cnt2 >= 1 ) break loop2;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(2, input);
                                throw eee;
                        }
                        cnt2++;
                    } while (true);

                    _last = (CommonTree)input.LT(1);
                    ID7=(CommonTree)match(input,ID,FOLLOW_ID_in_fieldAccess83); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_ID.add(ID7);


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: accesses, OBJECT_EXPR, VAR, ID
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: accesses
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_accesses=new RewriteRuleSubtreeStream(adaptor,"wildcard accesses",accesses);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 12:49: -> ^( FIELD ^( OBJECT_EXPR VAR $accesses) ID )
                    {
                        // JaqlTree.g:12:52: ^( FIELD ^( OBJECT_EXPR VAR $accesses) ID )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(FIELD, "FIELD"), root_1);

                        // JaqlTree.g:12:60: ^( OBJECT_EXPR VAR $accesses)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(stream_OBJECT_EXPR.nextNode(), root_2);

                        adaptor.addChild(root_2, stream_VAR.nextNode());
                        adaptor.addChild(root_2, stream_accesses.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }
                        adaptor.addChild(root_1, stream_ID.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;
                case 2 :
                    // JaqlTree.g:13:4: ^( OBJECT_EXPR VAR ID )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    OBJECT_EXPR8=(CommonTree)match(input,OBJECT_EXPR,FOLLOW_OBJECT_EXPR_in_fieldAccess107); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OBJECT_EXPR.add(OBJECT_EXPR8);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = OBJECT_EXPR8;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    VAR9=(CommonTree)match(input,VAR,FOLLOW_VAR_in_fieldAccess109); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_VAR.add(VAR9);

                    _last = (CommonTree)input.LT(1);
                    ID10=(CommonTree)match(input,ID,FOLLOW_ID_in_fieldAccess111); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_ID.add(ID10);


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: VAR, ID
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 13:26: -> ^( FIELD VAR ID )
                    {
                        // JaqlTree.g:13:29: ^( FIELD VAR ID )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(FIELD, "FIELD"), root_1);

                        adaptor.addChild(root_1, stream_VAR.nextNode());
                        adaptor.addChild(root_1, stream_ID.nextNode());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "fieldAccess"

    public static class arrayAccess_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // JaqlTree.g:15:1: arrayAccess : ( ^( OBJECT_EXPR VAR (accesses= . )+ ^( ARRAY_ACCESS (qualifier= . )+ ) ) -> ^( ARRAY_ACCESS ^( OBJECT_EXPR VAR $accesses) $qualifier) | ^( OBJECT_EXPR VAR ^( ARRAY_ACCESS (qualifier= . )+ ) ) -> ^( ARRAY_ACCESS VAR $qualifier) );
    public final JaqlTree.arrayAccess_return arrayAccess() throws RecognitionException {
        JaqlTree.arrayAccess_return retval = new JaqlTree.arrayAccess_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree OBJECT_EXPR11=null;
        CommonTree VAR12=null;
        CommonTree ARRAY_ACCESS13=null;
        CommonTree OBJECT_EXPR14=null;
        CommonTree VAR15=null;
        CommonTree ARRAY_ACCESS16=null;
        CommonTree accesses=null;
        CommonTree qualifier=null;

        CommonTree OBJECT_EXPR11_tree=null;
        CommonTree VAR12_tree=null;
        CommonTree ARRAY_ACCESS13_tree=null;
        CommonTree OBJECT_EXPR14_tree=null;
        CommonTree VAR15_tree=null;
        CommonTree ARRAY_ACCESS16_tree=null;
        CommonTree accesses_tree=null;
        CommonTree qualifier_tree=null;
        RewriteRuleNodeStream stream_VAR=new RewriteRuleNodeStream(adaptor,"token VAR");
        RewriteRuleNodeStream stream_ARRAY_ACCESS=new RewriteRuleNodeStream(adaptor,"token ARRAY_ACCESS");
        RewriteRuleNodeStream stream_OBJECT_EXPR=new RewriteRuleNodeStream(adaptor,"token OBJECT_EXPR");

        try {
            // JaqlTree.g:15:13: ( ^( OBJECT_EXPR VAR (accesses= . )+ ^( ARRAY_ACCESS (qualifier= . )+ ) ) -> ^( ARRAY_ACCESS ^( OBJECT_EXPR VAR $accesses) $qualifier) | ^( OBJECT_EXPR VAR ^( ARRAY_ACCESS (qualifier= . )+ ) ) -> ^( ARRAY_ACCESS VAR $qualifier) )
            int alt7=2;
            alt7 = dfa7.predict(input);
            switch (alt7) {
                case 1 :
                    // JaqlTree.g:15:15: ^( OBJECT_EXPR VAR (accesses= . )+ ^( ARRAY_ACCESS (qualifier= . )+ ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    OBJECT_EXPR11=(CommonTree)match(input,OBJECT_EXPR,FOLLOW_OBJECT_EXPR_in_arrayAccess132); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OBJECT_EXPR.add(OBJECT_EXPR11);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = OBJECT_EXPR11;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    VAR12=(CommonTree)match(input,VAR,FOLLOW_VAR_in_arrayAccess134); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_VAR.add(VAR12);

                    // JaqlTree.g:15:41: (accesses= . )+
                    int cnt4=0;
                    loop4:
                    do {
                        int alt4=2;
                        alt4 = dfa4.predict(input);
                        switch (alt4) {
                    	case 1 :
                    	    // JaqlTree.g:15:41: accesses= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    accesses=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_1==null ) _first_1 = accesses;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    if ( cnt4 >= 1 ) break loop4;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(4, input);
                                throw eee;
                        }
                        cnt4++;
                    } while (true);

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    ARRAY_ACCESS13=(CommonTree)match(input,ARRAY_ACCESS,FOLLOW_ARRAY_ACCESS_in_arrayAccess142); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_ARRAY_ACCESS.add(ARRAY_ACCESS13);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = ARRAY_ACCESS13;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    // JaqlTree.g:15:69: (qualifier= . )+
                    int cnt5=0;
                    loop5:
                    do {
                        int alt5=2;
                        int LA5_0 = input.LA(1);

                        if ( ((LA5_0>=SCRIPT && LA5_0<=66)) ) {
                            alt5=1;
                        }


                        switch (alt5) {
                    	case 1 :
                    	    // JaqlTree.g:15:69: qualifier= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    qualifier=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_2==null ) _first_2 = qualifier;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    if ( cnt5 >= 1 ) break loop5;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(5, input);
                                throw eee;
                        }
                        cnt5++;
                    } while (true);


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: qualifier, OBJECT_EXPR, ARRAY_ACCESS, VAR, accesses
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: accesses, qualifier
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_accesses=new RewriteRuleSubtreeStream(adaptor,"wildcard accesses",accesses);
                    RewriteRuleSubtreeStream stream_qualifier=new RewriteRuleSubtreeStream(adaptor,"wildcard qualifier",qualifier);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 15:75: -> ^( ARRAY_ACCESS ^( OBJECT_EXPR VAR $accesses) $qualifier)
                    {
                        // JaqlTree.g:15:78: ^( ARRAY_ACCESS ^( OBJECT_EXPR VAR $accesses) $qualifier)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_ARRAY_ACCESS.nextNode(), root_1);

                        // JaqlTree.g:15:93: ^( OBJECT_EXPR VAR $accesses)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(stream_OBJECT_EXPR.nextNode(), root_2);

                        adaptor.addChild(root_2, stream_VAR.nextNode());
                        adaptor.addChild(root_2, stream_accesses.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }
                        adaptor.addChild(root_1, stream_qualifier.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;
                case 2 :
                    // JaqlTree.g:16:4: ^( OBJECT_EXPR VAR ^( ARRAY_ACCESS (qualifier= . )+ ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    OBJECT_EXPR14=(CommonTree)match(input,OBJECT_EXPR,FOLLOW_OBJECT_EXPR_in_arrayAccess173); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OBJECT_EXPR.add(OBJECT_EXPR14);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = OBJECT_EXPR14;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    VAR15=(CommonTree)match(input,VAR,FOLLOW_VAR_in_arrayAccess175); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_VAR.add(VAR15);

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    ARRAY_ACCESS16=(CommonTree)match(input,ARRAY_ACCESS,FOLLOW_ARRAY_ACCESS_in_arrayAccess178); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_ARRAY_ACCESS.add(ARRAY_ACCESS16);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = ARRAY_ACCESS16;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    // JaqlTree.g:16:46: (qualifier= . )+
                    int cnt6=0;
                    loop6:
                    do {
                        int alt6=2;
                        int LA6_0 = input.LA(1);

                        if ( ((LA6_0>=SCRIPT && LA6_0<=66)) ) {
                            alt6=1;
                        }


                        switch (alt6) {
                    	case 1 :
                    	    // JaqlTree.g:16:46: qualifier= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    qualifier=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_2==null ) _first_2 = qualifier;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    if ( cnt6 >= 1 ) break loop6;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(6, input);
                                throw eee;
                        }
                        cnt6++;
                    } while (true);


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: VAR, ARRAY_ACCESS, qualifier
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: qualifier
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_qualifier=new RewriteRuleSubtreeStream(adaptor,"wildcard qualifier",qualifier);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 16:52: -> ^( ARRAY_ACCESS VAR $qualifier)
                    {
                        // JaqlTree.g:16:55: ^( ARRAY_ACCESS VAR $qualifier)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_ARRAY_ACCESS.nextNode(), root_1);

                        adaptor.addChild(root_1, stream_VAR.nextNode());
                        adaptor.addChild(root_1, stream_qualifier.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class methodCalls_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "methodCalls"
    // JaqlTree.g:19:1: methodCalls : ( ^( OBJECT_EXPR VAR (accesses= . )+ ^( FUNCTION_CALL name= . (params= . )* ) ) -> ^( METHOD_CALL ^( OBJECT_EXPR VAR $accesses) $name $params) | ^( OBJECT_EXPR VAR ^( FUNCTION_CALL name= . (params= . )* ) ) -> ^( METHOD_CALL $name $params) );
    public final JaqlTree.methodCalls_return methodCalls() throws RecognitionException {
        JaqlTree.methodCalls_return retval = new JaqlTree.methodCalls_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree OBJECT_EXPR17=null;
        CommonTree VAR18=null;
        CommonTree FUNCTION_CALL19=null;
        CommonTree OBJECT_EXPR20=null;
        CommonTree VAR21=null;
        CommonTree FUNCTION_CALL22=null;
        CommonTree accesses=null;
        CommonTree name=null;
        CommonTree params=null;

        CommonTree OBJECT_EXPR17_tree=null;
        CommonTree VAR18_tree=null;
        CommonTree FUNCTION_CALL19_tree=null;
        CommonTree OBJECT_EXPR20_tree=null;
        CommonTree VAR21_tree=null;
        CommonTree FUNCTION_CALL22_tree=null;
        CommonTree accesses_tree=null;
        CommonTree name_tree=null;
        CommonTree params_tree=null;
        RewriteRuleNodeStream stream_VAR=new RewriteRuleNodeStream(adaptor,"token VAR");
        RewriteRuleNodeStream stream_OBJECT_EXPR=new RewriteRuleNodeStream(adaptor,"token OBJECT_EXPR");
        RewriteRuleNodeStream stream_FUNCTION_CALL=new RewriteRuleNodeStream(adaptor,"token FUNCTION_CALL");

        try {
            // JaqlTree.g:19:13: ( ^( OBJECT_EXPR VAR (accesses= . )+ ^( FUNCTION_CALL name= . (params= . )* ) ) -> ^( METHOD_CALL ^( OBJECT_EXPR VAR $accesses) $name $params) | ^( OBJECT_EXPR VAR ^( FUNCTION_CALL name= . (params= . )* ) ) -> ^( METHOD_CALL $name $params) )
            int alt11=2;
            alt11 = dfa11.predict(input);
            switch (alt11) {
                case 1 :
                    // JaqlTree.g:19:15: ^( OBJECT_EXPR VAR (accesses= . )+ ^( FUNCTION_CALL name= . (params= . )* ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    OBJECT_EXPR17=(CommonTree)match(input,OBJECT_EXPR,FOLLOW_OBJECT_EXPR_in_methodCalls206); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OBJECT_EXPR.add(OBJECT_EXPR17);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = OBJECT_EXPR17;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    VAR18=(CommonTree)match(input,VAR,FOLLOW_VAR_in_methodCalls208); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_VAR.add(VAR18);

                    // JaqlTree.g:19:41: (accesses= . )+
                    int cnt8=0;
                    loop8:
                    do {
                        int alt8=2;
                        alt8 = dfa8.predict(input);
                        switch (alt8) {
                    	case 1 :
                    	    // JaqlTree.g:19:41: accesses= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    accesses=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_1==null ) _first_1 = accesses;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    if ( cnt8 >= 1 ) break loop8;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(8, input);
                                throw eee;
                        }
                        cnt8++;
                    } while (true);

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    FUNCTION_CALL19=(CommonTree)match(input,FUNCTION_CALL,FOLLOW_FUNCTION_CALL_in_methodCalls216); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_FUNCTION_CALL.add(FUNCTION_CALL19);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = FUNCTION_CALL19;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    name=(CommonTree)input.LT(1);
                    matchAny(input); if (state.failed) return retval;
                     
                    if ( state.backtracking==1 )
                    if ( _first_2==null ) _first_2 = name;
                    // JaqlTree.g:19:74: (params= . )*
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( ((LA9_0>=SCRIPT && LA9_0<=66)) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // JaqlTree.g:19:74: params= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    params=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_2==null ) _first_2 = params;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    break loop9;
                        }
                    } while (true);


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: name, OBJECT_EXPR, accesses, VAR, params
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: accesses, name, params
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_accesses=new RewriteRuleSubtreeStream(adaptor,"wildcard accesses",accesses);
                    RewriteRuleSubtreeStream stream_name=new RewriteRuleSubtreeStream(adaptor,"wildcard name",name);
                    RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"wildcard params",params);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 19:80: -> ^( METHOD_CALL ^( OBJECT_EXPR VAR $accesses) $name $params)
                    {
                        // JaqlTree.g:19:83: ^( METHOD_CALL ^( OBJECT_EXPR VAR $accesses) $name $params)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(METHOD_CALL, "METHOD_CALL"), root_1);

                        // JaqlTree.g:19:97: ^( OBJECT_EXPR VAR $accesses)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(stream_OBJECT_EXPR.nextNode(), root_2);

                        adaptor.addChild(root_2, stream_VAR.nextNode());
                        adaptor.addChild(root_2, stream_accesses.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }
                        adaptor.addChild(root_1, stream_name.nextTree());
                        adaptor.addChild(root_1, stream_params.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;
                case 2 :
                    // JaqlTree.g:20:4: ^( OBJECT_EXPR VAR ^( FUNCTION_CALL name= . (params= . )* ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    OBJECT_EXPR20=(CommonTree)match(input,OBJECT_EXPR,FOLLOW_OBJECT_EXPR_in_methodCalls254); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OBJECT_EXPR.add(OBJECT_EXPR20);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = OBJECT_EXPR20;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    VAR21=(CommonTree)match(input,VAR,FOLLOW_VAR_in_methodCalls256); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_VAR.add(VAR21);

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    FUNCTION_CALL22=(CommonTree)match(input,FUNCTION_CALL,FOLLOW_FUNCTION_CALL_in_methodCalls259); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_FUNCTION_CALL.add(FUNCTION_CALL22);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = FUNCTION_CALL22;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    name=(CommonTree)input.LT(1);
                    matchAny(input); if (state.failed) return retval;
                     
                    if ( state.backtracking==1 )
                    if ( _first_2==null ) _first_2 = name;
                    // JaqlTree.g:20:51: (params= . )*
                    loop10:
                    do {
                        int alt10=2;
                        int LA10_0 = input.LA(1);

                        if ( ((LA10_0>=SCRIPT && LA10_0<=66)) ) {
                            alt10=1;
                        }


                        switch (alt10) {
                    	case 1 :
                    	    // JaqlTree.g:20:51: params= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    params=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_2==null ) _first_2 = params;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    break loop10;
                        }
                    } while (true);


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: params, name
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: name, params
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_name=new RewriteRuleSubtreeStream(adaptor,"wildcard name",name);
                    RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"wildcard params",params);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 20:57: -> ^( METHOD_CALL $name $params)
                    {
                        // JaqlTree.g:20:60: ^( METHOD_CALL $name $params)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot((CommonTree)adaptor.create(METHOD_CALL, "METHOD_CALL"), root_1);

                        adaptor.addChild(root_1, stream_name.nextTree());
                        adaptor.addChild(root_1, stream_params.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "methodCalls"

    public static class streaming_return extends TreeRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "streaming"
    // JaqlTree.g:22:1: streaming : ( ^( STREAM (ops= . )+ ^( FUNCTION_CALL (params= . )* ) ) -> ^( FUNCTION_CALL ^( STREAM $ops) $params) | ^( STREAM (ops= . )+ ^( OPERATOR (params= . )* ) ) -> ^( OPERATOR ^( STREAM $ops) $params) | ^( STREAM VAR ) -> VAR | ^( STREAM ^( FUNCTION_CALL (params= . )* ) ) -> ^( FUNCTION_CALL ( $params)* ) | ^( STREAM ^( OPERATOR (params= . )* ) ) -> ^( OPERATOR ( $params)* ) );
    public final JaqlTree.streaming_return streaming() throws RecognitionException {
        JaqlTree.streaming_return retval = new JaqlTree.streaming_return();
        retval.start = input.LT(1);

        CommonTree root_0 = null;

        CommonTree _first_0 = null;
        CommonTree _last = null;

        CommonTree STREAM23=null;
        CommonTree FUNCTION_CALL24=null;
        CommonTree STREAM25=null;
        CommonTree OPERATOR26=null;
        CommonTree STREAM27=null;
        CommonTree VAR28=null;
        CommonTree STREAM29=null;
        CommonTree FUNCTION_CALL30=null;
        CommonTree STREAM31=null;
        CommonTree OPERATOR32=null;
        CommonTree ops=null;
        CommonTree params=null;

        CommonTree STREAM23_tree=null;
        CommonTree FUNCTION_CALL24_tree=null;
        CommonTree STREAM25_tree=null;
        CommonTree OPERATOR26_tree=null;
        CommonTree STREAM27_tree=null;
        CommonTree VAR28_tree=null;
        CommonTree STREAM29_tree=null;
        CommonTree FUNCTION_CALL30_tree=null;
        CommonTree STREAM31_tree=null;
        CommonTree OPERATOR32_tree=null;
        CommonTree ops_tree=null;
        CommonTree params_tree=null;
        RewriteRuleNodeStream stream_VAR=new RewriteRuleNodeStream(adaptor,"token VAR");
        RewriteRuleNodeStream stream_STREAM=new RewriteRuleNodeStream(adaptor,"token STREAM");
        RewriteRuleNodeStream stream_FUNCTION_CALL=new RewriteRuleNodeStream(adaptor,"token FUNCTION_CALL");
        RewriteRuleNodeStream stream_OPERATOR=new RewriteRuleNodeStream(adaptor,"token OPERATOR");

        try {
            // JaqlTree.g:22:11: ( ^( STREAM (ops= . )+ ^( FUNCTION_CALL (params= . )* ) ) -> ^( FUNCTION_CALL ^( STREAM $ops) $params) | ^( STREAM (ops= . )+ ^( OPERATOR (params= . )* ) ) -> ^( OPERATOR ^( STREAM $ops) $params) | ^( STREAM VAR ) -> VAR | ^( STREAM ^( FUNCTION_CALL (params= . )* ) ) -> ^( FUNCTION_CALL ( $params)* ) | ^( STREAM ^( OPERATOR (params= . )* ) ) -> ^( OPERATOR ( $params)* ) )
            int alt18=5;
            alt18 = dfa18.predict(input);
            switch (alt18) {
                case 1 :
                    // JaqlTree.g:22:13: ^( STREAM (ops= . )+ ^( FUNCTION_CALL (params= . )* ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    STREAM23=(CommonTree)match(input,STREAM,FOLLOW_STREAM_in_streaming291); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_STREAM.add(STREAM23);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = STREAM23;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    // JaqlTree.g:22:25: (ops= . )+
                    int cnt12=0;
                    loop12:
                    do {
                        int alt12=2;
                        alt12 = dfa12.predict(input);
                        switch (alt12) {
                    	case 1 :
                    	    // JaqlTree.g:22:25: ops= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    ops=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_1==null ) _first_1 = ops;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    if ( cnt12 >= 1 ) break loop12;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(12, input);
                                throw eee;
                        }
                        cnt12++;
                    } while (true);

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    FUNCTION_CALL24=(CommonTree)match(input,FUNCTION_CALL,FOLLOW_FUNCTION_CALL_in_streaming299); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_FUNCTION_CALL.add(FUNCTION_CALL24);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = FUNCTION_CALL24;
                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); if (state.failed) return retval;
                        // JaqlTree.g:22:51: (params= . )*
                        loop13:
                        do {
                            int alt13=2;
                            int LA13_0 = input.LA(1);

                            if ( ((LA13_0>=SCRIPT && LA13_0<=66)) ) {
                                alt13=1;
                            }


                            switch (alt13) {
                        	case 1 :
                        	    // JaqlTree.g:22:51: params= .
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    params=(CommonTree)input.LT(1);
                        	    matchAny(input); if (state.failed) return retval;
                        	     
                        	    if ( state.backtracking==1 )
                        	    if ( _first_2==null ) _first_2 = params;

                        	    if ( state.backtracking==1 ) {
                        	    retval.tree = (CommonTree)_first_0;
                        	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                        	    }
                        	    break;

                        	default :
                        	    break loop13;
                            }
                        } while (true);


                        match(input, Token.UP, null); if (state.failed) return retval;
                    }_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: ops, STREAM, FUNCTION_CALL, params
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: params, ops
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"wildcard params",params);
                    RewriteRuleSubtreeStream stream_ops=new RewriteRuleSubtreeStream(adaptor,"wildcard ops",ops);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 22:57: -> ^( FUNCTION_CALL ^( STREAM $ops) $params)
                    {
                        // JaqlTree.g:22:60: ^( FUNCTION_CALL ^( STREAM $ops) $params)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_FUNCTION_CALL.nextNode(), root_1);

                        // JaqlTree.g:22:76: ^( STREAM $ops)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(stream_STREAM.nextNode(), root_2);

                        adaptor.addChild(root_2, stream_ops.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }
                        adaptor.addChild(root_1, stream_params.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;
                case 2 :
                    // JaqlTree.g:23:4: ^( STREAM (ops= . )+ ^( OPERATOR (params= . )* ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    STREAM25=(CommonTree)match(input,STREAM,FOLLOW_STREAM_in_streaming328); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_STREAM.add(STREAM25);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = STREAM25;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    // JaqlTree.g:23:16: (ops= . )+
                    int cnt14=0;
                    loop14:
                    do {
                        int alt14=2;
                        alt14 = dfa14.predict(input);
                        switch (alt14) {
                    	case 1 :
                    	    // JaqlTree.g:23:16: ops= .
                    	    {
                    	    _last = (CommonTree)input.LT(1);
                    	    ops=(CommonTree)input.LT(1);
                    	    matchAny(input); if (state.failed) return retval;
                    	     
                    	    if ( state.backtracking==1 )
                    	    if ( _first_1==null ) _first_1 = ops;

                    	    if ( state.backtracking==1 ) {
                    	    retval.tree = (CommonTree)_first_0;
                    	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                    	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                    	    }
                    	    break;

                    	default :
                    	    if ( cnt14 >= 1 ) break loop14;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(14, input);
                                throw eee;
                        }
                        cnt14++;
                    } while (true);

                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    OPERATOR26=(CommonTree)match(input,OPERATOR,FOLLOW_OPERATOR_in_streaming336); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OPERATOR.add(OPERATOR26);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = OPERATOR26;
                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); if (state.failed) return retval;
                        // JaqlTree.g:23:37: (params= . )*
                        loop15:
                        do {
                            int alt15=2;
                            int LA15_0 = input.LA(1);

                            if ( ((LA15_0>=SCRIPT && LA15_0<=66)) ) {
                                alt15=1;
                            }


                            switch (alt15) {
                        	case 1 :
                        	    // JaqlTree.g:23:37: params= .
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    params=(CommonTree)input.LT(1);
                        	    matchAny(input); if (state.failed) return retval;
                        	     
                        	    if ( state.backtracking==1 )
                        	    if ( _first_2==null ) _first_2 = params;

                        	    if ( state.backtracking==1 ) {
                        	    retval.tree = (CommonTree)_first_0;
                        	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                        	    }
                        	    break;

                        	default :
                        	    break loop15;
                            }
                        } while (true);


                        match(input, Token.UP, null); if (state.failed) return retval;
                    }_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: params, OPERATOR, STREAM, ops
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: params, ops
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"wildcard params",params);
                    RewriteRuleSubtreeStream stream_ops=new RewriteRuleSubtreeStream(adaptor,"wildcard ops",ops);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 23:43: -> ^( OPERATOR ^( STREAM $ops) $params)
                    {
                        // JaqlTree.g:23:46: ^( OPERATOR ^( STREAM $ops) $params)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_OPERATOR.nextNode(), root_1);

                        // JaqlTree.g:23:57: ^( STREAM $ops)
                        {
                        CommonTree root_2 = (CommonTree)adaptor.nil();
                        root_2 = (CommonTree)adaptor.becomeRoot(stream_STREAM.nextNode(), root_2);

                        adaptor.addChild(root_2, stream_ops.nextTree());

                        adaptor.addChild(root_1, root_2);
                        }
                        adaptor.addChild(root_1, stream_params.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;
                case 3 :
                    // JaqlTree.g:24:4: ^( STREAM VAR )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    STREAM27=(CommonTree)match(input,STREAM,FOLLOW_STREAM_in_streaming365); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_STREAM.add(STREAM27);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = STREAM27;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    VAR28=(CommonTree)match(input,VAR,FOLLOW_VAR_in_streaming367); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_VAR.add(VAR28);


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: VAR
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 24:18: -> VAR
                    {
                        adaptor.addChild(root_0, stream_VAR.nextNode());

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;
                case 4 :
                    // JaqlTree.g:25:4: ^( STREAM ^( FUNCTION_CALL (params= . )* ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    STREAM29=(CommonTree)match(input,STREAM,FOLLOW_STREAM_in_streaming378); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_STREAM.add(STREAM29);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = STREAM29;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    FUNCTION_CALL30=(CommonTree)match(input,FUNCTION_CALL,FOLLOW_FUNCTION_CALL_in_streaming381); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_FUNCTION_CALL.add(FUNCTION_CALL30);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = FUNCTION_CALL30;
                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); if (state.failed) return retval;
                        // JaqlTree.g:25:35: (params= . )*
                        loop16:
                        do {
                            int alt16=2;
                            int LA16_0 = input.LA(1);

                            if ( ((LA16_0>=SCRIPT && LA16_0<=66)) ) {
                                alt16=1;
                            }


                            switch (alt16) {
                        	case 1 :
                        	    // JaqlTree.g:25:35: params= .
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    params=(CommonTree)input.LT(1);
                        	    matchAny(input); if (state.failed) return retval;
                        	     
                        	    if ( state.backtracking==1 )
                        	    if ( _first_2==null ) _first_2 = params;

                        	    if ( state.backtracking==1 ) {
                        	    retval.tree = (CommonTree)_first_0;
                        	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                        	    }
                        	    break;

                        	default :
                        	    break loop16;
                            }
                        } while (true);


                        match(input, Token.UP, null); if (state.failed) return retval;
                    }_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: params, FUNCTION_CALL
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: params
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"wildcard params",params);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 25:41: -> ^( FUNCTION_CALL ( $params)* )
                    {
                        // JaqlTree.g:25:44: ^( FUNCTION_CALL ( $params)* )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_FUNCTION_CALL.nextNode(), root_1);

                        // JaqlTree.g:25:60: ( $params)*
                        while ( stream_params.hasNext() ) {
                            adaptor.addChild(root_1, stream_params.nextTree());

                        }
                        stream_params.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;
                case 5 :
                    // JaqlTree.g:26:4: ^( STREAM ^( OPERATOR (params= . )* ) )
                    {
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_1 = _last;
                    CommonTree _first_1 = null;
                    _last = (CommonTree)input.LT(1);
                    STREAM31=(CommonTree)match(input,STREAM,FOLLOW_STREAM_in_streaming404); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_STREAM.add(STREAM31);


                    if ( state.backtracking==1 )
                    if ( _first_0==null ) _first_0 = STREAM31;
                    match(input, Token.DOWN, null); if (state.failed) return retval;
                    _last = (CommonTree)input.LT(1);
                    {
                    CommonTree _save_last_2 = _last;
                    CommonTree _first_2 = null;
                    _last = (CommonTree)input.LT(1);
                    OPERATOR32=(CommonTree)match(input,OPERATOR,FOLLOW_OPERATOR_in_streaming407); if (state.failed) return retval; 
                    if ( state.backtracking==1 ) stream_OPERATOR.add(OPERATOR32);


                    if ( state.backtracking==1 )
                    if ( _first_1==null ) _first_1 = OPERATOR32;
                    if ( input.LA(1)==Token.DOWN ) {
                        match(input, Token.DOWN, null); if (state.failed) return retval;
                        // JaqlTree.g:26:30: (params= . )*
                        loop17:
                        do {
                            int alt17=2;
                            int LA17_0 = input.LA(1);

                            if ( ((LA17_0>=SCRIPT && LA17_0<=66)) ) {
                                alt17=1;
                            }


                            switch (alt17) {
                        	case 1 :
                        	    // JaqlTree.g:26:30: params= .
                        	    {
                        	    _last = (CommonTree)input.LT(1);
                        	    params=(CommonTree)input.LT(1);
                        	    matchAny(input); if (state.failed) return retval;
                        	     
                        	    if ( state.backtracking==1 )
                        	    if ( _first_2==null ) _first_2 = params;

                        	    if ( state.backtracking==1 ) {
                        	    retval.tree = (CommonTree)_first_0;
                        	    if ( adaptor.getParent(retval.tree)!=null && adaptor.isNil( adaptor.getParent(retval.tree) ) )
                        	        retval.tree = (CommonTree)adaptor.getParent(retval.tree);}
                        	    }
                        	    break;

                        	default :
                        	    break loop17;
                            }
                        } while (true);


                        match(input, Token.UP, null); if (state.failed) return retval;
                    }_last = _save_last_2;
                    }


                    match(input, Token.UP, null); if (state.failed) return retval;_last = _save_last_1;
                    }



                    // AST REWRITE
                    // elements: params, OPERATOR
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: params
                    if ( state.backtracking==1 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"wildcard params",params);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 26:36: -> ^( OPERATOR ( $params)* )
                    {
                        // JaqlTree.g:26:39: ^( OPERATOR ( $params)* )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(stream_OPERATOR.nextNode(), root_1);

                        // JaqlTree.g:26:50: ( $params)*
                        while ( stream_params.hasNext() ) {
                            adaptor.addChild(root_1, stream_params.nextTree());

                        }
                        stream_params.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
                    input.replaceChildren(adaptor.getParent(retval.start),
                                          adaptor.getChildIndex(retval.start),
                                          adaptor.getChildIndex(_last),
                                          retval.tree);}
                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return retval;
    }
    // $ANTLR end "streaming"

    // Delegated rules


    protected DFA1 dfa1 = new DFA1(this);
    protected DFA7 dfa7 = new DFA7(this);
    protected DFA4 dfa4 = new DFA4(this);
    protected DFA11 dfa11 = new DFA11(this);
    protected DFA8 dfa8 = new DFA8(this);
    protected DFA18 dfa18 = new DFA18(this);
    protected DFA12 dfa12 = new DFA12(this);
    protected DFA14 dfa14 = new DFA14(this);
    static final String DFA1_eotS =
        "\40\uffff";
    static final String DFA1_eofS =
        "\40\uffff";
    static final String DFA1_minS =
        "\1\11\1\2\1\uffff\1\21\1\4\4\2\1\uffff\1\4\3\2\2\4\1\3\2\4\2\2\1"+
        "\4\2\2\1\uffff\1\2\1\3\1\uffff\1\3\1\2\2\3";
    static final String DFA1_maxS =
        "\1\16\1\2\1\uffff\1\21\5\102\1\uffff\16\102\1\uffff\2\102\1\uffff"+
        "\4\102";
    static final String DFA1_acceptS =
        "\2\uffff\1\3\6\uffff\1\1\16\uffff\1\2\2\uffff\1\4\4\uffff";
    static final String DFA1_specialS =
        "\40\uffff}>";
    static final String[] DFA1_transitionS = {
            "\1\1\4\uffff\1\2",
            "\1\3",
            "",
            "\1\4",
            "\4\10\1\6\3\10\1\7\6\10\1\5\57\10",
            "\1\12\1\11\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\16\1\uffff\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\17\1\uffff\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\12\1\uffff\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "",
            "\77\20",
            "\1\12\1\11\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\21\1\uffff\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\22\1\uffff\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\77\23",
            "\77\24",
            "\1\25\77\20",
            "\77\26",
            "\77\27",
            "\1\30\1\32\77\31",
            "\1\33\1\34\77\24",
            "\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\30\1\36\77\35",
            "\1\33\1\37\77\27",
            "",
            "\1\30\1\32\77\31",
            "\1\30\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "",
            "\1\33\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\30\1\36\77\35",
            "\1\30\4\10\1\14\3\10\1\15\6\10\1\13\57\10",
            "\1\33\4\10\1\14\3\10\1\15\6\10\1\13\57\10"
    };

    static final short[] DFA1_eot = DFA.unpackEncodedString(DFA1_eotS);
    static final short[] DFA1_eof = DFA.unpackEncodedString(DFA1_eofS);
    static final char[] DFA1_min = DFA.unpackEncodedStringToUnsignedChars(DFA1_minS);
    static final char[] DFA1_max = DFA.unpackEncodedStringToUnsignedChars(DFA1_maxS);
    static final short[] DFA1_accept = DFA.unpackEncodedString(DFA1_acceptS);
    static final short[] DFA1_special = DFA.unpackEncodedString(DFA1_specialS);
    static final short[][] DFA1_transition;

    static {
        int numStates = DFA1_transitionS.length;
        DFA1_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA1_transition[i] = DFA.unpackEncodedString(DFA1_transitionS[i]);
        }
    }

    class DFA1 extends DFA {

        public DFA1(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 1;
            this.eot = DFA1_eot;
            this.eof = DFA1_eof;
            this.min = DFA1_min;
            this.max = DFA1_max;
            this.accept = DFA1_accept;
            this.special = DFA1_special;
            this.transition = DFA1_transition;
        }
        public String getDescription() {
            return "10:1: topdown : ( fieldAccess | methodCalls | streaming | arrayAccess );";
        }
    }
    static final String DFA7_eotS =
        "\12\uffff";
    static final String DFA7_eofS =
        "\12\uffff";
    static final String DFA7_minS =
        "\1\11\1\2\1\21\1\4\1\2\1\uffff\1\4\1\2\1\uffff\1\3";
    static final String DFA7_maxS =
        "\1\11\1\2\1\21\2\102\1\uffff\2\102\1\uffff\1\102";
    static final String DFA7_acceptS =
        "\5\uffff\1\1\2\uffff\1\2\1\uffff";
    static final String DFA7_specialS =
        "\12\uffff}>";
    static final String[] DFA7_transitionS = {
            "\1\1",
            "\1\2",
            "\1\3",
            "\10\5\1\4\66\5",
            "\1\6\1\uffff\77\5",
            "",
            "\77\7",
            "\1\10\1\11\77\7",
            "",
            "\1\10\77\5"
    };

    static final short[] DFA7_eot = DFA.unpackEncodedString(DFA7_eotS);
    static final short[] DFA7_eof = DFA.unpackEncodedString(DFA7_eofS);
    static final char[] DFA7_min = DFA.unpackEncodedStringToUnsignedChars(DFA7_minS);
    static final char[] DFA7_max = DFA.unpackEncodedStringToUnsignedChars(DFA7_maxS);
    static final short[] DFA7_accept = DFA.unpackEncodedString(DFA7_acceptS);
    static final short[] DFA7_special = DFA.unpackEncodedString(DFA7_specialS);
    static final short[][] DFA7_transition;

    static {
        int numStates = DFA7_transitionS.length;
        DFA7_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA7_transition[i] = DFA.unpackEncodedString(DFA7_transitionS[i]);
        }
    }

    class DFA7 extends DFA {

        public DFA7(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 7;
            this.eot = DFA7_eot;
            this.eof = DFA7_eof;
            this.min = DFA7_min;
            this.max = DFA7_max;
            this.accept = DFA7_accept;
            this.special = DFA7_special;
            this.transition = DFA7_transition;
        }
        public String getDescription() {
            return "15:1: arrayAccess : ( ^( OBJECT_EXPR VAR (accesses= . )+ ^( ARRAY_ACCESS (qualifier= . )+ ) ) -> ^( ARRAY_ACCESS ^( OBJECT_EXPR VAR $accesses) $qualifier) | ^( OBJECT_EXPR VAR ^( ARRAY_ACCESS (qualifier= . )+ ) ) -> ^( ARRAY_ACCESS VAR $qualifier) );";
        }
    }
    static final String DFA4_eotS =
        "\7\uffff";
    static final String DFA4_eofS =
        "\7\uffff";
    static final String DFA4_minS =
        "\1\4\1\2\1\uffff\1\4\1\2\1\uffff\1\3";
    static final String DFA4_maxS =
        "\2\102\1\uffff\2\102\1\uffff\1\102";
    static final String DFA4_acceptS =
        "\2\uffff\1\1\2\uffff\1\2\1\uffff";
    static final String DFA4_specialS =
        "\7\uffff}>";
    static final String[] DFA4_transitionS = {
            "\10\2\1\1\66\2",
            "\1\3\1\uffff\77\2",
            "",
            "\77\4",
            "\1\5\1\6\77\4",
            "",
            "\1\5\77\2"
    };

    static final short[] DFA4_eot = DFA.unpackEncodedString(DFA4_eotS);
    static final short[] DFA4_eof = DFA.unpackEncodedString(DFA4_eofS);
    static final char[] DFA4_min = DFA.unpackEncodedStringToUnsignedChars(DFA4_minS);
    static final char[] DFA4_max = DFA.unpackEncodedStringToUnsignedChars(DFA4_maxS);
    static final short[] DFA4_accept = DFA.unpackEncodedString(DFA4_acceptS);
    static final short[] DFA4_special = DFA.unpackEncodedString(DFA4_specialS);
    static final short[][] DFA4_transition;

    static {
        int numStates = DFA4_transitionS.length;
        DFA4_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA4_transition[i] = DFA.unpackEncodedString(DFA4_transitionS[i]);
        }
    }

    class DFA4 extends DFA {

        public DFA4(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 4;
            this.eot = DFA4_eot;
            this.eof = DFA4_eof;
            this.min = DFA4_min;
            this.max = DFA4_max;
            this.accept = DFA4_accept;
            this.special = DFA4_special;
            this.transition = DFA4_transition;
        }
        public String getDescription() {
            return "()+ loopback of 15:41: (accesses= . )+";
        }
    }
    static final String DFA11_eotS =
        "\13\uffff";
    static final String DFA11_eofS =
        "\13\uffff";
    static final String DFA11_minS =
        "\1\11\1\2\1\21\1\4\1\2\1\uffff\1\4\1\2\1\uffff\1\2\1\3";
    static final String DFA11_maxS =
        "\1\11\1\2\1\21\2\102\1\uffff\2\102\1\uffff\2\102";
    static final String DFA11_acceptS =
        "\5\uffff\1\1\2\uffff\1\2\2\uffff";
    static final String DFA11_specialS =
        "\13\uffff}>";
    static final String[] DFA11_transitionS = {
            "\1\1",
            "\1\2",
            "\1\3",
            "\4\5\1\4\72\5",
            "\1\6\1\uffff\77\5",
            "",
            "\77\7",
            "\1\10\1\12\77\11",
            "",
            "\1\10\1\12\77\11",
            "\1\10\77\5"
    };

    static final short[] DFA11_eot = DFA.unpackEncodedString(DFA11_eotS);
    static final short[] DFA11_eof = DFA.unpackEncodedString(DFA11_eofS);
    static final char[] DFA11_min = DFA.unpackEncodedStringToUnsignedChars(DFA11_minS);
    static final char[] DFA11_max = DFA.unpackEncodedStringToUnsignedChars(DFA11_maxS);
    static final short[] DFA11_accept = DFA.unpackEncodedString(DFA11_acceptS);
    static final short[] DFA11_special = DFA.unpackEncodedString(DFA11_specialS);
    static final short[][] DFA11_transition;

    static {
        int numStates = DFA11_transitionS.length;
        DFA11_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA11_transition[i] = DFA.unpackEncodedString(DFA11_transitionS[i]);
        }
    }

    class DFA11 extends DFA {

        public DFA11(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 11;
            this.eot = DFA11_eot;
            this.eof = DFA11_eof;
            this.min = DFA11_min;
            this.max = DFA11_max;
            this.accept = DFA11_accept;
            this.special = DFA11_special;
            this.transition = DFA11_transition;
        }
        public String getDescription() {
            return "19:1: methodCalls : ( ^( OBJECT_EXPR VAR (accesses= . )+ ^( FUNCTION_CALL name= . (params= . )* ) ) -> ^( METHOD_CALL ^( OBJECT_EXPR VAR $accesses) $name $params) | ^( OBJECT_EXPR VAR ^( FUNCTION_CALL name= . (params= . )* ) ) -> ^( METHOD_CALL $name $params) );";
        }
    }
    static final String DFA8_eotS =
        "\10\uffff";
    static final String DFA8_eofS =
        "\10\uffff";
    static final String DFA8_minS =
        "\1\4\1\2\1\uffff\1\4\1\2\1\uffff\1\2\1\3";
    static final String DFA8_maxS =
        "\2\102\1\uffff\2\102\1\uffff\2\102";
    static final String DFA8_acceptS =
        "\2\uffff\1\1\2\uffff\1\2\2\uffff";
    static final String DFA8_specialS =
        "\10\uffff}>";
    static final String[] DFA8_transitionS = {
            "\4\2\1\1\72\2",
            "\1\3\1\uffff\77\2",
            "",
            "\77\4",
            "\1\5\1\7\77\6",
            "",
            "\1\5\1\7\77\6",
            "\1\5\77\2"
    };

    static final short[] DFA8_eot = DFA.unpackEncodedString(DFA8_eotS);
    static final short[] DFA8_eof = DFA.unpackEncodedString(DFA8_eofS);
    static final char[] DFA8_min = DFA.unpackEncodedStringToUnsignedChars(DFA8_minS);
    static final char[] DFA8_max = DFA.unpackEncodedStringToUnsignedChars(DFA8_maxS);
    static final short[] DFA8_accept = DFA.unpackEncodedString(DFA8_acceptS);
    static final short[] DFA8_special = DFA.unpackEncodedString(DFA8_specialS);
    static final short[][] DFA8_transition;

    static {
        int numStates = DFA8_transitionS.length;
        DFA8_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA8_transition[i] = DFA.unpackEncodedString(DFA8_transitionS[i]);
        }
    }

    class DFA8 extends DFA {

        public DFA8(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 8;
            this.eot = DFA8_eot;
            this.eof = DFA8_eof;
            this.min = DFA8_min;
            this.max = DFA8_max;
            this.accept = DFA8_accept;
            this.special = DFA8_special;
            this.transition = DFA8_transition;
        }
        public String getDescription() {
            return "()+ loopback of 19:41: (accesses= . )+";
        }
    }
    static final String DFA18_eotS =
        "\35\uffff";
    static final String DFA18_eofS =
        "\35\uffff";
    static final String DFA18_minS =
        "\1\16\1\2\1\4\4\2\1\uffff\1\4\2\2\5\3\1\2\1\uffff\1\2\1\uffff\1"+
        "\4\1\2\1\uffff\1\2\1\uffff\4\3";
    static final String DFA18_maxS =
        "\1\16\1\2\5\102\1\uffff\11\102\1\uffff\1\102\1\uffff\2\102\1\uffff"+
        "\1\102\1\uffff\4\102";
    static final String DFA18_acceptS =
        "\7\uffff\1\3\11\uffff\1\4\1\uffff\1\5\2\uffff\1\1\1\uffff\1\2\4"+
        "\uffff";
    static final String DFA18_specialS =
        "\35\uffff}>";
    static final String[] DFA18_transitionS = {
            "\1\1",
            "\1\2",
            "\4\6\1\4\7\6\1\5\1\3\61\6",
            "\1\10\1\7\4\6\1\11\7\6\1\12\62\6",
            "\1\13\1\uffff\4\6\1\11\7\6\1\12\62\6",
            "\1\14\1\uffff\4\6\1\11\7\6\1\12\62\6",
            "\1\10\1\uffff\4\6\1\11\7\6\1\12\62\6",
            "",
            "\77\15",
            "\1\16\1\uffff\4\6\1\11\7\6\1\12\62\6",
            "\1\17\1\uffff\4\6\1\11\7\6\1\12\62\6",
            "\1\21\77\20",
            "\1\23\77\22",
            "\1\24\77\15",
            "\1\26\77\25",
            "\1\30\77\27",
            "\1\21\1\31\77\20",
            "",
            "\1\23\1\32\77\22",
            "",
            "\4\6\1\11\7\6\1\12\62\6",
            "\1\26\1\33\77\25",
            "",
            "\1\30\1\34\77\27",
            "",
            "\1\21\4\6\1\11\7\6\1\12\62\6",
            "\1\23\4\6\1\11\7\6\1\12\62\6",
            "\1\26\4\6\1\11\7\6\1\12\62\6",
            "\1\30\4\6\1\11\7\6\1\12\62\6"
    };

    static final short[] DFA18_eot = DFA.unpackEncodedString(DFA18_eotS);
    static final short[] DFA18_eof = DFA.unpackEncodedString(DFA18_eofS);
    static final char[] DFA18_min = DFA.unpackEncodedStringToUnsignedChars(DFA18_minS);
    static final char[] DFA18_max = DFA.unpackEncodedStringToUnsignedChars(DFA18_maxS);
    static final short[] DFA18_accept = DFA.unpackEncodedString(DFA18_acceptS);
    static final short[] DFA18_special = DFA.unpackEncodedString(DFA18_specialS);
    static final short[][] DFA18_transition;

    static {
        int numStates = DFA18_transitionS.length;
        DFA18_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA18_transition[i] = DFA.unpackEncodedString(DFA18_transitionS[i]);
        }
    }

    class DFA18 extends DFA {

        public DFA18(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 18;
            this.eot = DFA18_eot;
            this.eof = DFA18_eof;
            this.min = DFA18_min;
            this.max = DFA18_max;
            this.accept = DFA18_accept;
            this.special = DFA18_special;
            this.transition = DFA18_transition;
        }
        public String getDescription() {
            return "22:1: streaming : ( ^( STREAM (ops= . )+ ^( FUNCTION_CALL (params= . )* ) ) -> ^( FUNCTION_CALL ^( STREAM $ops) $params) | ^( STREAM (ops= . )+ ^( OPERATOR (params= . )* ) ) -> ^( OPERATOR ^( STREAM $ops) $params) | ^( STREAM VAR ) -> VAR | ^( STREAM ^( FUNCTION_CALL (params= . )* ) ) -> ^( FUNCTION_CALL ( $params)* ) | ^( STREAM ^( OPERATOR (params= . )* ) ) -> ^( OPERATOR ( $params)* ) );";
        }
    }
    static final String DFA12_eotS =
        "\7\uffff";
    static final String DFA12_eofS =
        "\7\uffff";
    static final String DFA12_minS =
        "\1\4\1\2\1\uffff\1\3\1\2\1\uffff\1\3";
    static final String DFA12_maxS =
        "\2\102\1\uffff\2\102\1\uffff\1\102";
    static final String DFA12_acceptS =
        "\2\uffff\1\1\2\uffff\1\2\1\uffff";
    static final String DFA12_specialS =
        "\7\uffff}>";
    static final String[] DFA12_transitionS = {
            "\4\2\1\1\72\2",
            "\1\3\1\uffff\77\2",
            "",
            "\1\5\77\4",
            "\1\5\1\6\77\4",
            "",
            "\1\5\77\2"
    };

    static final short[] DFA12_eot = DFA.unpackEncodedString(DFA12_eotS);
    static final short[] DFA12_eof = DFA.unpackEncodedString(DFA12_eofS);
    static final char[] DFA12_min = DFA.unpackEncodedStringToUnsignedChars(DFA12_minS);
    static final char[] DFA12_max = DFA.unpackEncodedStringToUnsignedChars(DFA12_maxS);
    static final short[] DFA12_accept = DFA.unpackEncodedString(DFA12_acceptS);
    static final short[] DFA12_special = DFA.unpackEncodedString(DFA12_specialS);
    static final short[][] DFA12_transition;

    static {
        int numStates = DFA12_transitionS.length;
        DFA12_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA12_transition[i] = DFA.unpackEncodedString(DFA12_transitionS[i]);
        }
    }

    class DFA12 extends DFA {

        public DFA12(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 12;
            this.eot = DFA12_eot;
            this.eof = DFA12_eof;
            this.min = DFA12_min;
            this.max = DFA12_max;
            this.accept = DFA12_accept;
            this.special = DFA12_special;
            this.transition = DFA12_transition;
        }
        public String getDescription() {
            return "()+ loopback of 22:25: (ops= . )+";
        }
    }
    static final String DFA14_eotS =
        "\7\uffff";
    static final String DFA14_eofS =
        "\7\uffff";
    static final String DFA14_minS =
        "\1\4\1\2\1\uffff\1\3\1\2\1\uffff\1\3";
    static final String DFA14_maxS =
        "\2\102\1\uffff\2\102\1\uffff\1\102";
    static final String DFA14_acceptS =
        "\2\uffff\1\1\2\uffff\1\2\1\uffff";
    static final String DFA14_specialS =
        "\7\uffff}>";
    static final String[] DFA14_transitionS = {
            "\14\2\1\1\62\2",
            "\1\3\1\uffff\77\2",
            "",
            "\1\5\77\4",
            "\1\5\1\6\77\4",
            "",
            "\1\5\77\2"
    };

    static final short[] DFA14_eot = DFA.unpackEncodedString(DFA14_eotS);
    static final short[] DFA14_eof = DFA.unpackEncodedString(DFA14_eofS);
    static final char[] DFA14_min = DFA.unpackEncodedStringToUnsignedChars(DFA14_minS);
    static final char[] DFA14_max = DFA.unpackEncodedStringToUnsignedChars(DFA14_maxS);
    static final short[] DFA14_accept = DFA.unpackEncodedString(DFA14_acceptS);
    static final short[] DFA14_special = DFA.unpackEncodedString(DFA14_specialS);
    static final short[][] DFA14_transition;

    static {
        int numStates = DFA14_transitionS.length;
        DFA14_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA14_transition[i] = DFA.unpackEncodedString(DFA14_transitionS[i]);
        }
    }

    class DFA14 extends DFA {

        public DFA14(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 14;
            this.eot = DFA14_eot;
            this.eof = DFA14_eof;
            this.min = DFA14_min;
            this.max = DFA14_max;
            this.accept = DFA14_accept;
            this.special = DFA14_special;
            this.transition = DFA14_transition;
        }
        public String getDescription() {
            return "()+ loopback of 23:16: (ops= . )+";
        }
    }
 

    public static final BitSet FOLLOW_fieldAccess_in_topdown53 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_methodCalls_in_topdown57 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_streaming_in_topdown61 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_topdown65 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_OBJECT_EXPR_in_fieldAccess74 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_VAR_in_fieldAccess76 = new BitSet(new long[]{0xFFFFFFFFFFFFFFF0L,0x0000000000000007L});
    public static final BitSet FOLLOW_ID_in_fieldAccess83 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_OBJECT_EXPR_in_fieldAccess107 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_VAR_in_fieldAccess109 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_ID_in_fieldAccess111 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_OBJECT_EXPR_in_arrayAccess132 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_VAR_in_arrayAccess134 = new BitSet(new long[]{0xFFFFFFFFFFFFFFF0L,0x0000000000000007L});
    public static final BitSet FOLLOW_ARRAY_ACCESS_in_arrayAccess142 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_OBJECT_EXPR_in_arrayAccess173 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_VAR_in_arrayAccess175 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_ARRAY_ACCESS_in_arrayAccess178 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_OBJECT_EXPR_in_methodCalls206 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_VAR_in_methodCalls208 = new BitSet(new long[]{0xFFFFFFFFFFFFFFF0L,0x0000000000000007L});
    public static final BitSet FOLLOW_FUNCTION_CALL_in_methodCalls216 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_OBJECT_EXPR_in_methodCalls254 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_VAR_in_methodCalls256 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_FUNCTION_CALL_in_methodCalls259 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_STREAM_in_streaming291 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_FUNCTION_CALL_in_streaming299 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_STREAM_in_streaming328 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_OPERATOR_in_streaming336 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_STREAM_in_streaming365 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_VAR_in_streaming367 = new BitSet(new long[]{0x0000000000000008L});
    public static final BitSet FOLLOW_STREAM_in_streaming378 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_FUNCTION_CALL_in_streaming381 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_STREAM_in_streaming404 = new BitSet(new long[]{0x0000000000000004L});
    public static final BitSet FOLLOW_OPERATOR_in_streaming407 = new BitSet(new long[]{0x0000000000000004L});

}