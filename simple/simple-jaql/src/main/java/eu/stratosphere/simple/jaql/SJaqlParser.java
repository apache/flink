// $ANTLR 3.3 Nov 30, 2010 12:46:29 /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g 2011-09-12 19:14:23
 
package eu.stratosphere.simple.jaql; 

import eu.stratosphere.sopremo.*;
import eu.stratosphere.simple.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

public class SJaqlParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "EXPRESSION", "OPERATOR", "VAR", "ID", "STAR", "DECIMAL", "STRING", "INTEGER", "UINT", "LOWER_LETTER", "UPPER_LETTER", "DIGIT", "SIGN", "COMMENT", "QUOTE", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'='", "'or'", "'||'", "'and'", "'&&'", "'in'", "'not in'", "'<='", "'>='", "'<'", "'>'", "'=='", "'!='", "'+'", "'-'", "'/'", "'++'", "'--'", "'!'", "'~'", "'.'", "'('", "')'", "','", "':'", "'{'", "'}'", "'true'", "'false'", "'['", "']'", "'read'", "'write'", "'to'"
    };
    public static final int EOF=-1;
    public static final int T__25=25;
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
    public static final int EXPRESSION=4;
    public static final int OPERATOR=5;
    public static final int VAR=6;
    public static final int ID=7;
    public static final int STAR=8;
    public static final int DECIMAL=9;
    public static final int STRING=10;
    public static final int INTEGER=11;
    public static final int UINT=12;
    public static final int LOWER_LETTER=13;
    public static final int UPPER_LETTER=14;
    public static final int DIGIT=15;
    public static final int SIGN=16;
    public static final int COMMENT=17;
    public static final int QUOTE=18;
    public static final int WS=19;
    public static final int UNICODE_ESC=20;
    public static final int OCTAL_ESC=21;
    public static final int ESC_SEQ=22;
    public static final int HEX_DIGIT=23;
    public static final int EXPONENT=24;

    // delegates
    // delegators


        public SJaqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public SJaqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
            this.state.ruleMemo = new HashMap[87+1];
             
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return SJaqlParser.tokenNames; }
    public String getGrammarFileName() { return "/home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g"; }


    private Map<String, Operator> variables = new HashMap<String, Operator>();
    private OperatorFactory operatorFactory = new OperatorFactory();
    private List<Sink> sinks = new ArrayList<Sink>();

    public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
    	throws RecognitionException
    {
    	throw e;
    }

    protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
    	throws RecognitionException
    {
    	// if next token is what we are looking for then "delete" this token
    	if ( mismatchIsUnwantedToken(input, ttype) )
    		throw new UnwantedTokenException(ttype, input);
    		
    	// can't recover with single token deletion, try insertion
    	if ( mismatchIsMissingToken(input, follow)  ){
    		Object inserted = getMissingSymbol(input, null, ttype, follow);
    		throw new MissingTokenException(ttype, input, inserted);
    	}
    		
    	throw new MismatchedTokenException(ttype, input);
    }

    public SopremoPlan parse() throws RecognitionException {  
        script();
        return new SopremoPlan(sinks);
    }

    private EvaluationExpression makePath(Token inputVar, String... path) {
    	List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();

      accesses.add(new InputSelection(inputIndexForVariable(inputVar)));	
    		
    	for(String fragment : path)
    		accesses.add(new ObjectAccess(fragment));
    	
    	return PathExpression.valueOf(accesses);
    }

    private Operator getVariable(Token variable) {
    	Operator op = variables.get(variable.getText());
    	if(op == null)
    		throw new IllegalArgumentException("Unknown variable:", new RecognitionException(variable.getInputStream()));
    	return op;
    }

    private int inputIndexForVariable(Token variable) {
      int index = ((operator_scope)operator_stack.peek()).aliasNames.indexOf(variable.getText());
      if(index == -1)
        index = ((operator_scope)operator_stack.peek()).inputNames.indexOf(variable.getText());
      if(index == -1)
    		throw new IllegalArgumentException("Unknown variable:", new RecognitionException(variable.getInputStream()));
    	return index;
    }

    private Number parseInt(String text) {
      BigInteger result = new BigInteger(text);
      if(result.bitLength() <= 31)
        return result.intValue();
      if(result.bitLength() <= 63)
        return result.longValue();
      return result;
    }

    //private EvaluationExpression[] getExpressions(List nodes


    public static class script_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "script"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:122:1: script : statement ( ';' statement )* ';' ->;
    public final SJaqlParser.script_return script() throws RecognitionException {
        SJaqlParser.script_return retval = new SJaqlParser.script_return();
        retval.start = input.LT(1);
        int script_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal2=null;
        Token char_literal4=null;
        SJaqlParser.statement_return statement1 = null;

        SJaqlParser.statement_return statement3 = null;


        EvaluationExpression char_literal2_tree=null;
        EvaluationExpression char_literal4_tree=null;
        RewriteRuleTokenStream stream_25=new RewriteRuleTokenStream(adaptor,"token 25");
        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 1) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:2: ( statement ( ';' statement )* ';' ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:5: statement ( ';' statement )* ';'
            {
            pushFollow(FOLLOW_statement_in_script125);
            statement1=statement();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:15: ( ';' statement )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==25) ) {
                    int LA1_1 = input.LA(2);

                    if ( ((LA1_1>=VAR && LA1_1<=ID)||(LA1_1>=57 && LA1_1<=58)) ) {
                        alt1=1;
                    }


                }


                switch (alt1) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:16: ';' statement
            	    {
            	    char_literal2=(Token)match(input,25,FOLLOW_25_in_script128); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_25.add(char_literal2);

            	    pushFollow(FOLLOW_statement_in_script130);
            	    statement3=statement();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_statement.add(statement3.getTree());

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            char_literal4=(Token)match(input,25,FOLLOW_25_in_script134); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_25.add(char_literal4);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 123:36: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 1, script_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "script"

    public static class statement_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "statement"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:125:1: statement : ( assignment | operator ) ->;
    public final SJaqlParser.statement_return statement() throws RecognitionException {
        SJaqlParser.statement_return retval = new SJaqlParser.statement_return();
        retval.start = input.LT(1);
        int statement_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.assignment_return assignment5 = null;

        SJaqlParser.operator_return operator6 = null;


        RewriteRuleSubtreeStream stream_assignment=new RewriteRuleSubtreeStream(adaptor,"rule assignment");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 2) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:2: ( ( assignment | operator ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:4: ( assignment | operator )
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:4: ( assignment | operator )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==VAR) ) {
                alt2=1;
            }
            else if ( (LA2_0==ID||(LA2_0>=57 && LA2_0<=58)) ) {
                alt2=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;
            }
            switch (alt2) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:5: assignment
                    {
                    pushFollow(FOLLOW_assignment_in_statement146);
                    assignment5=assignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_assignment.add(assignment5.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:18: operator
                    {
                    pushFollow(FOLLOW_operator_in_statement150);
                    operator6=operator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_operator.add(operator6.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 126:28: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 2, statement_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "statement"

    public static class assignment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "assignment"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:1: assignment : target= VAR '=' source= operator ->;
    public final SJaqlParser.assignment_return assignment() throws RecognitionException {
        SJaqlParser.assignment_return retval = new SJaqlParser.assignment_return();
        retval.start = input.LT(1);
        int assignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token target=null;
        Token char_literal7=null;
        SJaqlParser.operator_return source = null;


        EvaluationExpression target_tree=null;
        EvaluationExpression char_literal7_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_26=new RewriteRuleTokenStream(adaptor,"token 26");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 3) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:2: (target= VAR '=' source= operator ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:4: target= VAR '=' source= operator
            {
            target=(Token)match(input,VAR,FOLLOW_VAR_in_assignment166); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(target);

            char_literal7=(Token)match(input,26,FOLLOW_26_in_assignment168); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_26.add(char_literal7);

            pushFollow(FOLLOW_operator_in_assignment172);
            source=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(source.getTree());
            if ( state.backtracking==0 ) {
               variables.put((target!=null?target.getText():null), (source!=null?source.op:null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 129:80: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 3, assignment_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "assignment"

    public static class expression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:1: expression : orExpression ;
    public final SJaqlParser.expression_return expression() throws RecognitionException {
        SJaqlParser.expression_return retval = new SJaqlParser.expression_return();
        retval.start = input.LT(1);
        int expression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.orExpression_return orExpression8 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:132:2: ( orExpression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:132:4: orExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_orExpression_in_expression186);
            orExpression8=orExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, orExpression8.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 4, expression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "expression"

    public static class orExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:1: orExpression : exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) ;
    public final SJaqlParser.orExpression_return orExpression() throws RecognitionException {
        SJaqlParser.orExpression_return retval = new SJaqlParser.orExpression_return();
        retval.start = input.LT(1);
        int orExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal9=null;
        Token string_literal10=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal9_tree=null;
        EvaluationExpression string_literal10_tree=null;
        RewriteRuleTokenStream stream_27=new RewriteRuleTokenStream(adaptor,"token 27");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_andExpression=new RewriteRuleSubtreeStream(adaptor,"rule andExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 5) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:3: (exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:5: exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression198);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:26: ( ( 'or' | '||' ) exprs+= andExpression )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==27) ) {
                    int LA4_2 = input.LA(2);

                    if ( (synpred4_SJaql()) ) {
                        alt4=1;
                    }


                }
                else if ( (LA4_0==28) ) {
                    int LA4_3 = input.LA(2);

                    if ( (synpred4_SJaql()) ) {
                        alt4=1;
                    }


                }


                switch (alt4) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:27: ( 'or' | '||' ) exprs+= andExpression
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:27: ( 'or' | '||' )
            	    int alt3=2;
            	    int LA3_0 = input.LA(1);

            	    if ( (LA3_0==27) ) {
            	        alt3=1;
            	    }
            	    else if ( (LA3_0==28) ) {
            	        alt3=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 3, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt3) {
            	        case 1 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:28: 'or'
            	            {
            	            string_literal9=(Token)match(input,27,FOLLOW_27_in_orExpression202); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_27.add(string_literal9);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:35: '||'
            	            {
            	            string_literal10=(Token)match(input,28,FOLLOW_28_in_orExpression206); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_28.add(string_literal10);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_andExpression_in_orExpression211);
            	    exprs=andExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 136:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 137:3: -> ^( EXPRESSION[\"OrExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:137:6: ^( EXPRESSION[\"OrExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "OrExpression"), root_1);

                adaptor.addChild(root_1,  list_exprs.toArray(new EvaluationExpression[list_exprs.size()]) );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 5, orExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "orExpression"

    public static class andExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "andExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:1: andExpression : exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) ;
    public final SJaqlParser.andExpression_return andExpression() throws RecognitionException {
        SJaqlParser.andExpression_return retval = new SJaqlParser.andExpression_return();
        retval.start = input.LT(1);
        int andExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal11=null;
        Token string_literal12=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal11_tree=null;
        EvaluationExpression string_literal12_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:3: (exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:5: exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression245);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:30: ( ( 'and' | '&&' ) exprs+= elementExpression )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( (LA6_0==29) ) {
                    int LA6_2 = input.LA(2);

                    if ( (synpred6_SJaql()) ) {
                        alt6=1;
                    }


                }
                else if ( (LA6_0==30) ) {
                    int LA6_3 = input.LA(2);

                    if ( (synpred6_SJaql()) ) {
                        alt6=1;
                    }


                }


                switch (alt6) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:31: ( 'and' | '&&' ) exprs+= elementExpression
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:31: ( 'and' | '&&' )
            	    int alt5=2;
            	    int LA5_0 = input.LA(1);

            	    if ( (LA5_0==29) ) {
            	        alt5=1;
            	    }
            	    else if ( (LA5_0==30) ) {
            	        alt5=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 5, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt5) {
            	        case 1 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:32: 'and'
            	            {
            	            string_literal11=(Token)match(input,29,FOLLOW_29_in_andExpression249); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_29.add(string_literal11);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:40: '&&'
            	            {
            	            string_literal12=(Token)match(input,30,FOLLOW_30_in_andExpression253); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_30.add(string_literal12);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_elementExpression_in_andExpression258);
            	    exprs=elementExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop6;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 141:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 142:3: -> ^( EXPRESSION[\"AndExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:142:6: ^( EXPRESSION[\"AndExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "AndExpression"), root_1);

                adaptor.addChild(root_1,  list_exprs.toArray(new EvaluationExpression[list_exprs.size()]) );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 6, andExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "andExpression"

    public static class elementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "elementExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:1: elementExpression : ( comparisonExpression | elem= comparisonExpression 'in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) | elem= comparisonExpression 'not in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) );
    public final SJaqlParser.elementExpression_return elementExpression() throws RecognitionException {
        SJaqlParser.elementExpression_return retval = new SJaqlParser.elementExpression_return();
        retval.start = input.LT(1);
        int elementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal14=null;
        Token string_literal15=null;
        SJaqlParser.comparisonExpression_return elem = null;

        SJaqlParser.comparisonExpression_return set = null;

        SJaqlParser.comparisonExpression_return comparisonExpression13 = null;


        EvaluationExpression string_literal14_tree=null;
        EvaluationExpression string_literal15_tree=null;
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:2: ( comparisonExpression | elem= comparisonExpression 'in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) | elem= comparisonExpression 'not in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            int alt7=3;
            alt7 = dfa7.predict(input);
            switch (alt7) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:4: comparisonExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression290);
                    comparisonExpression13=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, comparisonExpression13.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:146:4: elem= comparisonExpression 'in' set= comparisonExpression
                    {
                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression297);
                    elem=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
                    string_literal14=(Token)match(input,31,FOLLOW_31_in_elementExpression299); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(string_literal14);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression303);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());


                    // AST REWRITE
                    // elements: elem, set
                    // token labels: 
                    // rule labels: elem, retval, set
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_elem=new RewriteRuleSubtreeStream(adaptor,"rule elem",elem!=null?elem.tree:null);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_set=new RewriteRuleSubtreeStream(adaptor,"rule set",set!=null?set.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 146:60: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:146:63: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ElementInSetExpression"), root_1);

                        adaptor.addChild(root_1, stream_elem.nextTree());
                        adaptor.addChild(root_1, ElementInSetExpression.Quantor.EXISTS_IN);
                        adaptor.addChild(root_1, stream_set.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:147:4: elem= comparisonExpression 'not in' set= comparisonExpression
                    {
                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression325);
                    elem=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
                    string_literal15=(Token)match(input,32,FOLLOW_32_in_elementExpression327); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(string_literal15);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression331);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());


                    // AST REWRITE
                    // elements: elem, set
                    // token labels: 
                    // rule labels: elem, retval, set
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_elem=new RewriteRuleSubtreeStream(adaptor,"rule elem",elem!=null?elem.tree:null);
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_set=new RewriteRuleSubtreeStream(adaptor,"rule set",set!=null?set.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 147:64: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:147:67: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ElementInSetExpression"), root_1);

                        adaptor.addChild(root_1, stream_elem.nextTree());
                        adaptor.addChild(root_1, ElementInSetExpression.Quantor.EXISTS_NOT_IN);
                        adaptor.addChild(root_1, stream_set.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 7, elementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "elementExpression"

    public static class comparisonExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
    public final SJaqlParser.comparisonExpression_return comparisonExpression() throws RecognitionException {
        SJaqlParser.comparisonExpression_return retval = new SJaqlParser.comparisonExpression_return();
        retval.start = input.LT(1);
        int comparisonExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.arithmeticExpression_return e1 = null;

        SJaqlParser.arithmeticExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_36=new RewriteRuleTokenStream(adaptor,"token 36");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleTokenStream stream_37=new RewriteRuleTokenStream(adaptor,"token 37");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_arithmeticExpression=new RewriteRuleSubtreeStream(adaptor,"rule arithmeticExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:2: (e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:4: e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression358);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:28: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            int alt9=2;
            switch ( input.LA(1) ) {
                case 33:
                    {
                    int LA9_1 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt9=1;
                    }
                    }
                    break;
                case 34:
                    {
                    int LA9_2 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt9=1;
                    }
                    }
                    break;
                case 35:
                    {
                    int LA9_3 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt9=1;
                    }
                    }
                    break;
                case 36:
                    {
                    int LA9_4 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt9=1;
                    }
                    }
                    break;
                case 37:
                    {
                    int LA9_5 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt9=1;
                    }
                    }
                    break;
                case 38:
                    {
                    int LA9_6 = input.LA(2);

                    if ( (synpred14_SJaql()) ) {
                        alt9=1;
                    }
                    }
                    break;
            }

            switch (alt9) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
                    int alt8=6;
                    switch ( input.LA(1) ) {
                    case 33:
                        {
                        alt8=1;
                        }
                        break;
                    case 34:
                        {
                        alt8=2;
                        }
                        break;
                    case 35:
                        {
                        alt8=3;
                        }
                        break;
                    case 36:
                        {
                        alt8=4;
                        }
                        break;
                    case 37:
                        {
                        alt8=5;
                        }
                        break;
                    case 38:
                        {
                        alt8=6;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 8, 0, input);

                        throw nvae;
                    }

                    switch (alt8) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:30: s= '<='
                            {
                            s=(Token)match(input,33,FOLLOW_33_in_comparisonExpression364); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_33.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:39: s= '>='
                            {
                            s=(Token)match(input,34,FOLLOW_34_in_comparisonExpression370); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_34.add(s);


                            }
                            break;
                        case 3 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:48: s= '<'
                            {
                            s=(Token)match(input,35,FOLLOW_35_in_comparisonExpression376); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_35.add(s);


                            }
                            break;
                        case 4 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:56: s= '>'
                            {
                            s=(Token)match(input,36,FOLLOW_36_in_comparisonExpression382); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_36.add(s);


                            }
                            break;
                        case 5 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:64: s= '=='
                            {
                            s=(Token)match(input,37,FOLLOW_37_in_comparisonExpression388); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_37.add(s);


                            }
                            break;
                        case 6 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:73: s= '!='
                            {
                            s=(Token)match(input,38,FOLLOW_38_in_comparisonExpression394); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_38.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression399);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e1, e2, e1, e2, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 151:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }
            else // 152:2: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:36: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 153:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:153:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.valueOfSymbol((s!=null?s.getText():null)));
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 8, comparisonExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "comparisonExpression"

    public static class arithmeticExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arithmeticExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:155:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final SJaqlParser.arithmeticExpression_return arithmeticExpression() throws RecognitionException {
        SJaqlParser.arithmeticExpression_return retval = new SJaqlParser.arithmeticExpression_return();
        retval.start = input.LT(1);
        int arithmeticExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.multiplicationExpression_return e1 = null;

        SJaqlParser.multiplicationExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_multiplicationExpression=new RewriteRuleSubtreeStream(adaptor,"rule multiplicationExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 9) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:2: (e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:4: e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression457);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:32: ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==39) ) {
                int LA11_1 = input.LA(2);

                if ( (synpred16_SJaql()) ) {
                    alt11=1;
                }
            }
            else if ( (LA11_0==40) ) {
                int LA11_2 = input.LA(2);

                if ( (synpred16_SJaql()) ) {
                    alt11=1;
                }
            }
            switch (alt11) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' ) e2= multiplicationExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' )
                    int alt10=2;
                    int LA10_0 = input.LA(1);

                    if ( (LA10_0==39) ) {
                        alt10=1;
                    }
                    else if ( (LA10_0==40) ) {
                        alt10=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 10, 0, input);

                        throw nvae;
                    }
                    switch (alt10) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:34: s= '+'
                            {
                            s=(Token)match(input,39,FOLLOW_39_in_arithmeticExpression463); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_39.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:42: s= '-'
                            {
                            s=(Token)match(input,40,FOLLOW_40_in_arithmeticExpression469); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_40.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression474);
                    e2=multiplicationExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multiplicationExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e1, e2, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 157:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 159:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 9, arithmeticExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arithmeticExpression"

    public static class multiplicationExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiplicationExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:161:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final SJaqlParser.multiplicationExpression_return multiplicationExpression() throws RecognitionException {
        SJaqlParser.multiplicationExpression_return retval = new SJaqlParser.multiplicationExpression_return();
        retval.start = input.LT(1);
        int multiplicationExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.preincrementExpression_return e1 = null;

        SJaqlParser.preincrementExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_preincrementExpression=new RewriteRuleSubtreeStream(adaptor,"rule preincrementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 10) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:2: (e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:4: e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression517);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:30: ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0==STAR) ) {
                int LA13_1 = input.LA(2);

                if ( (synpred18_SJaql()) ) {
                    alt13=1;
                }
            }
            else if ( (LA13_0==41) ) {
                int LA13_2 = input.LA(2);

                if ( (synpred18_SJaql()) ) {
                    alt13=1;
                }
            }
            switch (alt13) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' ) e2= preincrementExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' )
                    int alt12=2;
                    int LA12_0 = input.LA(1);

                    if ( (LA12_0==STAR) ) {
                        alt12=1;
                    }
                    else if ( (LA12_0==41) ) {
                        alt12=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 12, 0, input);

                        throw nvae;
                    }
                    switch (alt12) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression523); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:40: s= '/'
                            {
                            s=(Token)match(input,41,FOLLOW_41_in_multiplicationExpression529); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression534);
                    e2=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_preincrementExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e1, e1, e2
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 163:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:163:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 165:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 10, multiplicationExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "multiplicationExpression"

    public static class preincrementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "preincrementExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:167:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
    public final SJaqlParser.preincrementExpression_return preincrementExpression() throws RecognitionException {
        SJaqlParser.preincrementExpression_return retval = new SJaqlParser.preincrementExpression_return();
        retval.start = input.LT(1);
        int preincrementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal16=null;
        Token string_literal18=null;
        SJaqlParser.preincrementExpression_return preincrementExpression17 = null;

        SJaqlParser.preincrementExpression_return preincrementExpression19 = null;

        SJaqlParser.unaryExpression_return unaryExpression20 = null;


        EvaluationExpression string_literal16_tree=null;
        EvaluationExpression string_literal18_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 11) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
            int alt14=3;
            switch ( input.LA(1) ) {
            case 42:
                {
                alt14=1;
                }
                break;
            case 43:
                {
                alt14=2;
                }
                break;
            case VAR:
            case ID:
            case DECIMAL:
            case STRING:
            case INTEGER:
            case UINT:
            case 44:
            case 45:
            case 47:
            case 51:
            case 53:
            case 54:
            case 55:
            case 57:
            case 58:
                {
                alt14=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }

            switch (alt14) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal16=(Token)match(input,42,FOLLOW_42_in_preincrementExpression575); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal16_tree = (EvaluationExpression)adaptor.create(string_literal16);
                    adaptor.addChild(root_0, string_literal16_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression577);
                    preincrementExpression17=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression17.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:169:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal18=(Token)match(input,43,FOLLOW_43_in_preincrementExpression582); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal18_tree = (EvaluationExpression)adaptor.create(string_literal18);
                    adaptor.addChild(root_0, string_literal18_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression584);
                    preincrementExpression19=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression19.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression589);
                    unaryExpression20=unaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression20.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 11, preincrementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "preincrementExpression"

    public static class unaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:172:1: unaryExpression : ( '!' | '~' )? pathExpression ;
    public final SJaqlParser.unaryExpression_return unaryExpression() throws RecognitionException {
        SJaqlParser.unaryExpression_return retval = new SJaqlParser.unaryExpression_return();
        retval.start = input.LT(1);
        int unaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token set21=null;
        SJaqlParser.pathExpression_return pathExpression22 = null;


        EvaluationExpression set21_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:173:2: ( ( '!' | '~' )? pathExpression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:173:4: ( '!' | '~' )? pathExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:173:4: ( '!' | '~' )?
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( ((LA15_0>=44 && LA15_0<=45)) ) {
                alt15=1;
            }
            switch (alt15) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:
                    {
                    set21=(Token)input.LT(1);
                    if ( (input.LA(1)>=44 && input.LA(1)<=45) ) {
                        input.consume();
                        if ( state.backtracking==0 ) adaptor.addChild(root_0, (EvaluationExpression)adaptor.create(set21));
                        state.errorRecovery=false;state.failed=false;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }


                    }
                    break;

            }

            pushFollow(FOLLOW_pathExpression_in_unaryExpression608);
            pathExpression22=pathExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, pathExpression22.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 12, unaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "unaryExpression"

    protected static class pathExpression_scope {
        List<EvaluationExpression> fragments;
    }
    protected Stack pathExpression_stack = new Stack();

    public static class pathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "pathExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:1: pathExpression : ( valueExpression ( ( '.' ( ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );
    public final SJaqlParser.pathExpression_return pathExpression() throws RecognitionException {
        pathExpression_stack.push(new pathExpression_scope());
        SJaqlParser.pathExpression_return retval = new SJaqlParser.pathExpression_return();
        retval.start = input.LT(1);
        int pathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal24=null;
        Token ID25=null;
        SJaqlParser.valueExpression_return valueExpression23 = null;

        SJaqlParser.arrayAccess_return arrayAccess26 = null;

        SJaqlParser.valueExpression_return valueExpression27 = null;


        EvaluationExpression char_literal24_tree=null;
        EvaluationExpression ID25_tree=null;
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
         ((pathExpression_scope)pathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 13) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:3: ( valueExpression ( ( '.' ( ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression )
            int alt17=2;
            alt17 = dfa17.predict(input);
            switch (alt17) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:5: valueExpression ( ( '.' ( ID ) ) | arrayAccess )+
                    {
                    pushFollow(FOLLOW_valueExpression_in_pathExpression631);
                    valueExpression23=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(valueExpression23.getTree());
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:21: ( ( '.' ( ID ) ) | arrayAccess )+
                    int cnt16=0;
                    loop16:
                    do {
                        int alt16=3;
                        int LA16_0 = input.LA(1);

                        if ( (LA16_0==46) ) {
                            int LA16_2 = input.LA(2);

                            if ( (synpred23_SJaql()) ) {
                                alt16=1;
                            }


                        }
                        else if ( (LA16_0==55) ) {
                            int LA16_3 = input.LA(2);

                            if ( (synpred24_SJaql()) ) {
                                alt16=2;
                            }


                        }


                        switch (alt16) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( '.' ( ID ) )
                    	    {
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( '.' ( ID ) )
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:6: '.' ( ID )
                    	    {
                    	    char_literal24=(Token)match(input,46,FOLLOW_46_in_pathExpression640); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_46.add(char_literal24);

                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:10: ( ID )
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:11: ID
                    	    {
                    	    ID25=(Token)match(input,ID,FOLLOW_ID_in_pathExpression643); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(ID25);

                    	    if ( state.backtracking==0 ) {
                    	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(new ObjectAccess((ID25!=null?ID25.getText():null))); 
                    	    }

                    	    }


                    	    }


                    	    }
                    	    break;
                    	case 2 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: arrayAccess
                    	    {
                    	    pushFollow(FOLLOW_arrayAccess_in_pathExpression667);
                    	    arrayAccess26=arrayAccess();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess26.getTree());
                    	    if ( state.backtracking==0 ) {
                    	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add((arrayAccess26!=null?((EvaluationExpression)arrayAccess26.tree):null)); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt16 >= 1 ) break loop16;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(16, input);
                                throw eee;
                        }
                        cnt16++;
                    } while (true);

                    if ( state.backtracking==0 ) {
                       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(0, (valueExpression23!=null?((EvaluationExpression)valueExpression23.tree):null)); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 184:139: -> ^( EXPRESSION[\"PathExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:143: ^( EXPRESSION[\"PathExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "PathExpression"), root_1);

                        adaptor.addChild(root_1,  ((pathExpression_scope)pathExpression_stack.peek()).fragments );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:7: valueExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_valueExpression_in_pathExpression693);
                    valueExpression27=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, valueExpression27.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 13, pathExpression_StartIndex); }
            pathExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "pathExpression"

    public static class valueExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "valueExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:187:1: valueExpression : ( functionCall | parenthesesExpression | literal | VAR -> | arrayCreation | objectCreation | operatorExpression );
    public final SJaqlParser.valueExpression_return valueExpression() throws RecognitionException {
        SJaqlParser.valueExpression_return retval = new SJaqlParser.valueExpression_return();
        retval.start = input.LT(1);
        int valueExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR31=null;
        SJaqlParser.functionCall_return functionCall28 = null;

        SJaqlParser.parenthesesExpression_return parenthesesExpression29 = null;

        SJaqlParser.literal_return literal30 = null;

        SJaqlParser.arrayCreation_return arrayCreation32 = null;

        SJaqlParser.objectCreation_return objectCreation33 = null;

        SJaqlParser.operatorExpression_return operatorExpression34 = null;


        EvaluationExpression VAR31_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:2: ( functionCall | parenthesesExpression | literal | VAR -> | arrayCreation | objectCreation | operatorExpression )
            int alt18=7;
            switch ( input.LA(1) ) {
            case ID:
                {
                int LA18_1 = input.LA(2);

                if ( (LA18_1==47) ) {
                    alt18=1;
                }
                else if ( (LA18_1==VAR) ) {
                    alt18=7;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 18, 1, input);

                    throw nvae;
                }
                }
                break;
            case 47:
                {
                alt18=2;
                }
                break;
            case DECIMAL:
            case STRING:
            case INTEGER:
            case UINT:
            case 53:
            case 54:
                {
                alt18=3;
                }
                break;
            case VAR:
                {
                alt18=4;
                }
                break;
            case 55:
                {
                alt18=5;
                }
                break;
            case 51:
                {
                alt18=6;
                }
                break;
            case 57:
            case 58:
                {
                alt18=7;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 18, 0, input);

                throw nvae;
            }

            switch (alt18) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:4: functionCall
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_functionCall_in_valueExpression702);
                    functionCall28=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionCall28.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression708);
                    parenthesesExpression29=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression29.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:190:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_valueExpression714);
                    literal30=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal30.getTree());

                    }
                    break;
                case 4 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:191:4: VAR
                    {
                    VAR31=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression720); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR31);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 191:8: ->
                    {
                        adaptor.addChild(root_0,  makePath(VAR31) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_arrayCreation_in_valueExpression729);
                    arrayCreation32=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation32.getTree());

                    }
                    break;
                case 6 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:193:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_objectCreation_in_valueExpression735);
                    objectCreation33=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation33.getTree());

                    }
                    break;
                case 7 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:194:4: operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_operatorExpression_in_valueExpression741);
                    operatorExpression34=operatorExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operatorExpression34.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 14, valueExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "valueExpression"

    public static class operatorExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:196:1: operatorExpression : operator ;
    public final SJaqlParser.operatorExpression_return operatorExpression() throws RecognitionException {
        SJaqlParser.operatorExpression_return retval = new SJaqlParser.operatorExpression_return();
        retval.start = input.LT(1);
        int operatorExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.operator_return operator35 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:197:2: ( operator )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:197:4: operator
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_operator_in_operatorExpression751);
            operator35=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, operator35.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 15, operatorExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "operatorExpression"

    public static class parenthesesExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "parenthesesExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
    public final SJaqlParser.parenthesesExpression_return parenthesesExpression() throws RecognitionException {
        SJaqlParser.parenthesesExpression_return retval = new SJaqlParser.parenthesesExpression_return();
        retval.start = input.LT(1);
        int parenthesesExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal36=null;
        Token char_literal38=null;
        SJaqlParser.expression_return expression37 = null;


        EvaluationExpression char_literal36_tree=null;
        EvaluationExpression char_literal38_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 16) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:2: ( ( '(' expression ')' ) -> expression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:4: ( '(' expression ')' )
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:4: ( '(' expression ')' )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:5: '(' expression ')'
            {
            char_literal36=(Token)match(input,47,FOLLOW_47_in_parenthesesExpression763); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_47.add(char_literal36);

            pushFollow(FOLLOW_expression_in_parenthesesExpression765);
            expression37=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression37.getTree());
            char_literal38=(Token)match(input,48,FOLLOW_48_in_parenthesesExpression767); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_48.add(char_literal38);


            }



            // AST REWRITE
            // elements: expression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 200:25: -> expression
            {
                adaptor.addChild(root_0, stream_expression.nextTree());

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 16, parenthesesExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "parenthesesExpression"

    public static class functionCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionCall"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:1: functionCall : ID '(' (params+= expression ( ',' params+= expression )* ) ')' -> ^( EXPRESSION[\"FunctionCall\"] $params) ;
    public final SJaqlParser.functionCall_return functionCall() throws RecognitionException {
        SJaqlParser.functionCall_return retval = new SJaqlParser.functionCall_return();
        retval.start = input.LT(1);
        int functionCall_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token ID39=null;
        Token char_literal40=null;
        Token char_literal41=null;
        Token char_literal42=null;
        List list_params=null;
        RuleReturnScope params = null;
        EvaluationExpression ID39_tree=null;
        EvaluationExpression char_literal40_tree=null;
        EvaluationExpression char_literal41_tree=null;
        EvaluationExpression char_literal42_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 17) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:2: ( ID '(' (params+= expression ( ',' params+= expression )* ) ')' -> ^( EXPRESSION[\"FunctionCall\"] $params) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:4: ID '(' (params+= expression ( ',' params+= expression )* ) ')'
            {
            ID39=(Token)match(input,ID,FOLLOW_ID_in_functionCall781); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(ID39);

            char_literal40=(Token)match(input,47,FOLLOW_47_in_functionCall783); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_47.add(char_literal40);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:11: (params+= expression ( ',' params+= expression )* )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:12: params+= expression ( ',' params+= expression )*
            {
            pushFollow(FOLLOW_expression_in_functionCall788);
            params=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(params.getTree());
            if (list_params==null) list_params=new ArrayList();
            list_params.add(params.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:31: ( ',' params+= expression )*
            loop19:
            do {
                int alt19=2;
                int LA19_0 = input.LA(1);

                if ( (LA19_0==49) ) {
                    alt19=1;
                }


                switch (alt19) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:32: ',' params+= expression
            	    {
            	    char_literal41=(Token)match(input,49,FOLLOW_49_in_functionCall791); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_49.add(char_literal41);

            	    pushFollow(FOLLOW_expression_in_functionCall795);
            	    params=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(params.getTree());
            	    if (list_params==null) list_params=new ArrayList();
            	    list_params.add(params.getTree());


            	    }
            	    break;

            	default :
            	    break loop19;
                }
            } while (true);


            }

            char_literal42=(Token)match(input,48,FOLLOW_48_in_functionCall800); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_48.add(char_literal42);



            // AST REWRITE
            // elements: params
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: params
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_params=new RewriteRuleSubtreeStream(adaptor,"token params",list_params);
            root_0 = (EvaluationExpression)adaptor.nil();
            // 203:62: -> ^( EXPRESSION[\"FunctionCall\"] $params)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:65: ^( EXPRESSION[\"FunctionCall\"] $params)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "FunctionCall"), root_1);

                adaptor.addChild(root_1, stream_params.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 17, functionCall_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "functionCall"

    public static class fieldAssignment_return extends ParserRuleReturnScope {
        public ObjectCreation.Mapping mapping;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fieldAssignment"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:205:1: fieldAssignment returns [ObjectCreation.Mapping mapping] : ( VAR '.' STAR -> | VAR '.' ID -> | ID ':' expression ->);
    public final SJaqlParser.fieldAssignment_return fieldAssignment() throws RecognitionException {
        SJaqlParser.fieldAssignment_return retval = new SJaqlParser.fieldAssignment_return();
        retval.start = input.LT(1);
        int fieldAssignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR43=null;
        Token char_literal44=null;
        Token STAR45=null;
        Token VAR46=null;
        Token char_literal47=null;
        Token ID48=null;
        Token ID49=null;
        Token char_literal50=null;
        SJaqlParser.expression_return expression51 = null;


        EvaluationExpression VAR43_tree=null;
        EvaluationExpression char_literal44_tree=null;
        EvaluationExpression STAR45_tree=null;
        EvaluationExpression VAR46_tree=null;
        EvaluationExpression char_literal47_tree=null;
        EvaluationExpression ID48_tree=null;
        EvaluationExpression ID49_tree=null;
        EvaluationExpression char_literal50_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 18) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:2: ( VAR '.' STAR -> | VAR '.' ID -> | ID ':' expression ->)
            int alt20=3;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==VAR) ) {
                int LA20_1 = input.LA(2);

                if ( (LA20_1==46) ) {
                    int LA20_3 = input.LA(3);

                    if ( (LA20_3==STAR) ) {
                        alt20=1;
                    }
                    else if ( (LA20_3==ID) ) {
                        alt20=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 20, 3, input);

                        throw nvae;
                    }
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 20, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA20_0==ID) ) {
                alt20=3;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 20, 0, input);

                throw nvae;
            }
            switch (alt20) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:4: VAR '.' STAR
                    {
                    VAR43=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment824); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR43);

                    char_literal44=(Token)match(input,46,FOLLOW_46_in_fieldAssignment826); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_46.add(char_literal44);

                    STAR45=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment828); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR45);

                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.CopyFields(makePath(VAR43))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 206:99: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:207:4: VAR '.' ID
                    {
                    VAR46=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment837); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR46);

                    char_literal47=(Token)match(input,46,FOLLOW_46_in_fieldAssignment839); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_46.add(char_literal47);

                    ID48=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment841); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID48);

                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.Mapping((ID48!=null?ID48.getText():null), makePath(VAR46, (ID48!=null?ID48.getText():null)))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 207:114: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:208:4: ID ':' expression
                    {
                    ID49=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment850); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID49);

                    char_literal50=(Token)match(input,50,FOLLOW_50_in_fieldAssignment852); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_50.add(char_literal50);

                    pushFollow(FOLLOW_expression_in_fieldAssignment854);
                    expression51=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression51.getTree());
                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.Mapping((ID49!=null?ID49.getText():null), (expression51!=null?((EvaluationExpression)expression51.tree):null))); 
                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 208:113: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 18, fieldAssignment_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "fieldAssignment"

    protected static class objectCreation_scope {
        List<ObjectCreation.Mapping> mappings;
    }
    protected Stack objectCreation_stack = new Stack();

    public static class objectCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "objectCreation"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
    public final SJaqlParser.objectCreation_return objectCreation() throws RecognitionException {
        objectCreation_stack.push(new objectCreation_scope());
        SJaqlParser.objectCreation_return retval = new SJaqlParser.objectCreation_return();
        retval.start = input.LT(1);
        int objectCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal52=null;
        Token char_literal54=null;
        Token char_literal56=null;
        Token char_literal57=null;
        SJaqlParser.fieldAssignment_return fieldAssignment53 = null;

        SJaqlParser.fieldAssignment_return fieldAssignment55 = null;


        EvaluationExpression char_literal52_tree=null;
        EvaluationExpression char_literal54_tree=null;
        EvaluationExpression char_literal56_tree=null;
        EvaluationExpression char_literal57_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 19) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal52=(Token)match(input,51,FOLLOW_51_in_objectCreation876); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_51.add(char_literal52);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( ((LA23_0>=VAR && LA23_0<=ID)) ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation879);
                    fieldAssignment53=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment53.getTree());
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:25: ( ',' fieldAssignment )*
                    loop21:
                    do {
                        int alt21=2;
                        int LA21_0 = input.LA(1);

                        if ( (LA21_0==49) ) {
                            int LA21_1 = input.LA(2);

                            if ( ((LA21_1>=VAR && LA21_1<=ID)) ) {
                                alt21=1;
                            }


                        }


                        switch (alt21) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:26: ',' fieldAssignment
                    	    {
                    	    char_literal54=(Token)match(input,49,FOLLOW_49_in_objectCreation882); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_49.add(char_literal54);

                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation884);
                    	    fieldAssignment55=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment55.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop21;
                        }
                    } while (true);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:48: ( ',' )?
                    int alt22=2;
                    int LA22_0 = input.LA(1);

                    if ( (LA22_0==49) ) {
                        alt22=1;
                    }
                    switch (alt22) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                            {
                            char_literal56=(Token)match(input,49,FOLLOW_49_in_objectCreation888); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(char_literal56);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal57=(Token)match(input,52,FOLLOW_52_in_objectCreation893); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_52.add(char_literal57);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 213:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:62: ^( EXPRESSION[\"ObjectCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ObjectCreation"), root_1);

                adaptor.addChild(root_1,  ((objectCreation_scope)objectCreation_stack.peek()).mappings );

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 19, objectCreation_StartIndex); }
            objectCreation_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "objectCreation"

    public static class literal_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "literal"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) );
    public final SJaqlParser.literal_return literal() throws RecognitionException {
        SJaqlParser.literal_return retval = new SJaqlParser.literal_return();
        retval.start = input.LT(1);
        int literal_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token val=null;

        EvaluationExpression val_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_DECIMAL=new RewriteRuleTokenStream(adaptor,"token DECIMAL");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_54=new RewriteRuleTokenStream(adaptor,"token 54");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 20) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:220:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) )
            int alt24=6;
            switch ( input.LA(1) ) {
            case 53:
                {
                alt24=1;
                }
                break;
            case 54:
                {
                alt24=2;
                }
                break;
            case DECIMAL:
                {
                alt24=3;
                }
                break;
            case STRING:
                {
                alt24=4;
                }
                break;
            case INTEGER:
                {
                alt24=5;
                }
                break;
            case UINT:
                {
                alt24=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 24, 0, input);

                throw nvae;
            }

            switch (alt24) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:220:4: val= 'true'
                    {
                    val=(Token)match(input,53,FOLLOW_53_in_literal917); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_53.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 220:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:220:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  Boolean.TRUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:221:4: val= 'false'
                    {
                    val=(Token)match(input,54,FOLLOW_54_in_literal933); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_54.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 221:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:221:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  Boolean.FALSE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:222:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal949); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DECIMAL.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 222:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:222:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  new BigDecimal((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 4 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal965); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 223:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  (val!=null?val.getText():null) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:224:5: val= INTEGER
                    {
                    val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal982); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_INTEGER.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 224:17: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:224:20: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  parseInt((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:225:5: val= UINT
                    {
                    val=(Token)match(input,UINT,FOLLOW_UINT_in_literal999); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_UINT.add(val);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 225:14: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:225:17: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  parseInt((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 20, literal_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "literal"

    public static class arrayAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:227:1: arrayAccess : ( '[' pos= ( INTEGER | UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' start= ( INTEGER | UINT ) ':' end= ( INTEGER | UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
    public final SJaqlParser.arrayAccess_return arrayAccess() throws RecognitionException {
        SJaqlParser.arrayAccess_return retval = new SJaqlParser.arrayAccess_return();
        retval.start = input.LT(1);
        int arrayAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token pos=null;
        Token start=null;
        Token end=null;
        Token char_literal58=null;
        Token INTEGER59=null;
        Token UINT60=null;
        Token char_literal61=null;
        Token char_literal62=null;
        Token INTEGER63=null;
        Token UINT64=null;
        Token char_literal65=null;
        Token INTEGER66=null;
        Token UINT67=null;
        Token char_literal68=null;
        Token char_literal69=null;
        Token STAR70=null;
        Token char_literal71=null;

        EvaluationExpression pos_tree=null;
        EvaluationExpression start_tree=null;
        EvaluationExpression end_tree=null;
        EvaluationExpression char_literal58_tree=null;
        EvaluationExpression INTEGER59_tree=null;
        EvaluationExpression UINT60_tree=null;
        EvaluationExpression char_literal61_tree=null;
        EvaluationExpression char_literal62_tree=null;
        EvaluationExpression INTEGER63_tree=null;
        EvaluationExpression UINT64_tree=null;
        EvaluationExpression char_literal65_tree=null;
        EvaluationExpression INTEGER66_tree=null;
        EvaluationExpression UINT67_tree=null;
        EvaluationExpression char_literal68_tree=null;
        EvaluationExpression char_literal69_tree=null;
        EvaluationExpression STAR70_tree=null;
        EvaluationExpression char_literal71_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 21) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:3: ( '[' pos= ( INTEGER | UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' start= ( INTEGER | UINT ) ':' end= ( INTEGER | UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt28=3;
            int LA28_0 = input.LA(1);

            if ( (LA28_0==55) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt28=3;
                    }
                    break;
                case INTEGER:
                    {
                    int LA28_3 = input.LA(3);

                    if ( (LA28_3==56) ) {
                        alt28=1;
                    }
                    else if ( (LA28_3==50) ) {
                        alt28=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 28, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case UINT:
                    {
                    int LA28_4 = input.LA(3);

                    if ( (LA28_4==56) ) {
                        alt28=1;
                    }
                    else if ( (LA28_4==50) ) {
                        alt28=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 28, 4, input);

                        throw nvae;
                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 28, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 28, 0, input);

                throw nvae;
            }
            switch (alt28) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:6: '[' pos= ( INTEGER | UINT ) ']'
                    {
                    char_literal58=(Token)match(input,55,FOLLOW_55_in_arrayAccess1019); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_55.add(char_literal58);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:14: ( INTEGER | UINT )
                    int alt25=2;
                    int LA25_0 = input.LA(1);

                    if ( (LA25_0==INTEGER) ) {
                        alt25=1;
                    }
                    else if ( (LA25_0==UINT) ) {
                        alt25=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 25, 0, input);

                        throw nvae;
                    }
                    switch (alt25) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:15: INTEGER
                            {
                            INTEGER59=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1024); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(INTEGER59);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:25: UINT
                            {
                            UINT60=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1028); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(UINT60);


                            }
                            break;

                    }

                    char_literal61=(Token)match(input,56,FOLLOW_56_in_arrayAccess1031); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal61);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 228:35: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:38: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((pos!=null?pos.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:6: '[' start= ( INTEGER | UINT ) ':' end= ( INTEGER | UINT ) ']'
                    {
                    char_literal62=(Token)match(input,55,FOLLOW_55_in_arrayAccess1047); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_55.add(char_literal62);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:16: ( INTEGER | UINT )
                    int alt26=2;
                    int LA26_0 = input.LA(1);

                    if ( (LA26_0==INTEGER) ) {
                        alt26=1;
                    }
                    else if ( (LA26_0==UINT) ) {
                        alt26=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 26, 0, input);

                        throw nvae;
                    }
                    switch (alt26) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:17: INTEGER
                            {
                            INTEGER63=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1052); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(INTEGER63);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:27: UINT
                            {
                            UINT64=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1056); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(UINT64);


                            }
                            break;

                    }

                    char_literal65=(Token)match(input,50,FOLLOW_50_in_arrayAccess1059); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_50.add(char_literal65);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:41: ( INTEGER | UINT )
                    int alt27=2;
                    int LA27_0 = input.LA(1);

                    if ( (LA27_0==INTEGER) ) {
                        alt27=1;
                    }
                    else if ( (LA27_0==UINT) ) {
                        alt27=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 27, 0, input);

                        throw nvae;
                    }
                    switch (alt27) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:42: INTEGER
                            {
                            INTEGER66=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1064); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(INTEGER66);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:52: UINT
                            {
                            UINT67=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1068); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(UINT67);


                            }
                            break;

                    }

                    char_literal68=(Token)match(input,56,FOLLOW_56_in_arrayAccess1071); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal68);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 229:62: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:65: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((start!=null?start.getText():null)) );
                        adaptor.addChild(root_1,  Integer.valueOf((end!=null?end.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:6: '[' STAR ']'
                    {
                    char_literal69=(Token)match(input,55,FOLLOW_55_in_arrayAccess1089); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_55.add(char_literal69);

                    STAR70=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1091); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR70);

                    char_literal71=(Token)match(input,56,FOLLOW_56_in_arrayAccess1093); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal71);



                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 230:19: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:22: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 21, arrayAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayCreation"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:232:1: arrayCreation : '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] $elems) ;
    public final SJaqlParser.arrayCreation_return arrayCreation() throws RecognitionException {
        SJaqlParser.arrayCreation_return retval = new SJaqlParser.arrayCreation_return();
        retval.start = input.LT(1);
        int arrayCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal72=null;
        Token char_literal73=null;
        Token char_literal74=null;
        Token char_literal75=null;
        List list_elems=null;
        RuleReturnScope elems = null;
        EvaluationExpression char_literal72_tree=null;
        EvaluationExpression char_literal73_tree=null;
        EvaluationExpression char_literal74_tree=null;
        EvaluationExpression char_literal75_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 22) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:2: ( '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] $elems) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:5: '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']'
            {
            char_literal72=(Token)match(input,55,FOLLOW_55_in_arrayCreation1111); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_55.add(char_literal72);

            pushFollow(FOLLOW_expression_in_arrayCreation1115);
            elems=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            if (list_elems==null) list_elems=new ArrayList();
            list_elems.add(elems.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:27: ( ',' elems+= expression )*
            loop29:
            do {
                int alt29=2;
                int LA29_0 = input.LA(1);

                if ( (LA29_0==49) ) {
                    int LA29_1 = input.LA(2);

                    if ( ((LA29_1>=VAR && LA29_1<=ID)||(LA29_1>=DECIMAL && LA29_1<=UINT)||(LA29_1>=42 && LA29_1<=45)||LA29_1==47||LA29_1==51||(LA29_1>=53 && LA29_1<=55)||(LA29_1>=57 && LA29_1<=58)) ) {
                        alt29=1;
                    }


                }


                switch (alt29) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:28: ',' elems+= expression
            	    {
            	    char_literal73=(Token)match(input,49,FOLLOW_49_in_arrayCreation1118); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_49.add(char_literal73);

            	    pushFollow(FOLLOW_expression_in_arrayCreation1122);
            	    elems=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            	    if (list_elems==null) list_elems=new ArrayList();
            	    list_elems.add(elems.getTree());


            	    }
            	    break;

            	default :
            	    break loop29;
                }
            } while (true);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:52: ( ',' )?
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==49) ) {
                alt30=1;
            }
            switch (alt30) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                    {
                    char_literal74=(Token)match(input,49,FOLLOW_49_in_arrayCreation1126); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal74);


                    }
                    break;

            }

            char_literal75=(Token)match(input,56,FOLLOW_56_in_arrayCreation1129); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_56.add(char_literal75);



            // AST REWRITE
            // elements: elems
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: elems
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_elems=new RewriteRuleSubtreeStream(adaptor,"token elems",list_elems);
            root_0 = (EvaluationExpression)adaptor.nil();
            // 233:61: -> ^( EXPRESSION[\"ArrayCreation\"] $elems)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:64: ^( EXPRESSION[\"ArrayCreation\"] $elems)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayCreation"), root_1);

                adaptor.addChild(root_1, stream_elems.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 22, arrayCreation_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayCreation"

    protected static class operator_scope {
        List<Operator> inputOperators;
        List<String> inputNames;
        List<String> aliasNames;
        Operator result;
    }
    protected Stack operator_stack = new Stack();

    public static class operator_return extends ParserRuleReturnScope {
        public Operator op=null;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:1: operator returns [Operator op=null] : opRule= ( readOperator | writeOperator | genericOperator ) ;
    public final SJaqlParser.operator_return operator() throws RecognitionException {
        operator_stack.push(new operator_scope());
        SJaqlParser.operator_return retval = new SJaqlParser.operator_return();
        retval.start = input.LT(1);
        int operator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token opRule=null;
        SJaqlParser.readOperator_return readOperator76 = null;

        SJaqlParser.writeOperator_return writeOperator77 = null;

        SJaqlParser.genericOperator_return genericOperator78 = null;


        EvaluationExpression opRule_tree=null;

         
        	((operator_scope)operator_stack.peek()).inputOperators = new ArrayList<Operator>();
          ((operator_scope)operator_stack.peek()).inputNames = new ArrayList<String>();
          ((operator_scope)operator_stack.peek()).aliasNames = new ArrayList<String>();

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 23) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:2: (opRule= ( readOperator | writeOperator | genericOperator ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:4: opRule= ( readOperator | writeOperator | genericOperator )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:11: ( readOperator | writeOperator | genericOperator )
            int alt31=3;
            switch ( input.LA(1) ) {
            case 57:
                {
                alt31=1;
                }
                break;
            case 58:
                {
                alt31=2;
                }
                break;
            case ID:
                {
                alt31=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;
            }

            switch (alt31) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:12: readOperator
                    {
                    pushFollow(FOLLOW_readOperator_in_operator1164);
                    readOperator76=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator76.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:27: writeOperator
                    {
                    pushFollow(FOLLOW_writeOperator_in_operator1168);
                    writeOperator77=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator77.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:43: genericOperator
                    {
                    pushFollow(FOLLOW_genericOperator_in_operator1172);
                    genericOperator78=genericOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, genericOperator78.getTree());

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
                retval.op = ((operator_scope)operator_stack.peek()).result;

            }

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 23, operator_StartIndex); }
            operator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operator"

    public static class readOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "readOperator"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:252:1: readOperator : 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.readOperator_return readOperator() throws RecognitionException {
        SJaqlParser.readOperator_return retval = new SJaqlParser.readOperator_return();
        retval.start = input.LT(1);
        int readOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token loc=null;
        Token file=null;
        Token string_literal79=null;
        Token char_literal80=null;
        Token char_literal81=null;

        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal79_tree=null;
        EvaluationExpression char_literal80_tree=null;
        EvaluationExpression char_literal81_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 24) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:2: ( 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:4: 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal79=(Token)match(input,57,FOLLOW_57_in_readOperator1186); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_57.add(string_literal79);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:11: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==ID) ) {
                int LA33_1 = input.LA(2);

                if ( (LA33_1==47) ) {
                    alt33=2;
                }
                else if ( (LA33_1==STRING) ) {
                    alt33=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 33, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA33_0==STRING) ) {
                alt33=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 33, 0, input);

                throw nvae;
            }
            switch (alt33) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:12: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:15: (loc= ID )?
                    int alt32=2;
                    int LA32_0 = input.LA(1);

                    if ( (LA32_0==ID) ) {
                        alt32=1;
                    }
                    switch (alt32) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1191); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1196); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:34: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1202); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal80=(Token)match(input,47,FOLLOW_47_in_readOperator1204); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_47.add(char_literal80);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1208); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal81=(Token)match(input,48,FOLLOW_48_in_readOperator1210); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal81);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               ((operator_scope)operator_stack.peek()).result = new Source(JsonInputFormat.class, (file!=null?file.getText():null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 253:133: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 24, readOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "readOperator"

    public static class writeOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "writeOperator"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:1: writeOperator : 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.writeOperator_return writeOperator() throws RecognitionException {
        SJaqlParser.writeOperator_return retval = new SJaqlParser.writeOperator_return();
        retval.start = input.LT(1);
        int writeOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token loc=null;
        Token file=null;
        Token string_literal82=null;
        Token string_literal83=null;
        Token char_literal84=null;
        Token char_literal85=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal82_tree=null;
        EvaluationExpression string_literal83_tree=null;
        EvaluationExpression char_literal84_tree=null;
        EvaluationExpression char_literal85_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 25) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:2: ( 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:4: 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal82=(Token)match(input,58,FOLLOW_58_in_writeOperator1224); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_58.add(string_literal82);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator1228); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            string_literal83=(Token)match(input,59,FOLLOW_59_in_writeOperator1230); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(string_literal83);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:26: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt35=2;
            int LA35_0 = input.LA(1);

            if ( (LA35_0==ID) ) {
                int LA35_1 = input.LA(2);

                if ( (LA35_1==47) ) {
                    alt35=2;
                }
                else if ( (LA35_1==STRING) ) {
                    alt35=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 35, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA35_0==STRING) ) {
                alt35=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 35, 0, input);

                throw nvae;
            }
            switch (alt35) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:27: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:30: (loc= ID )?
                    int alt34=2;
                    int LA34_0 = input.LA(1);

                    if ( (LA34_0==ID) ) {
                        alt34=1;
                    }
                    switch (alt34) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1235); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1240); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:49: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1246); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal84=(Token)match(input,47,FOLLOW_47_in_writeOperator1248); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_47.add(char_literal84);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1252); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal85=(Token)match(input,48,FOLLOW_48_in_writeOperator1254); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal85);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
              	Sink sink = new Sink(JsonOutputFormat.class, (file!=null?file.getText():null), getVariable(from));
                ((operator_scope)operator_stack.peek()).result = sink;
                this.sinks.add(sink);

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 261:3: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 25, writeOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "writeOperator"

    protected static class genericOperator_scope {
        OperatorFactory.OperatorInfo operatorInfo;
    }
    protected Stack genericOperator_stack = new Stack();

    public static class genericOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "genericOperator"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:263:1: genericOperator : name= ID input ( ',' input )* ( operatorOption )* ->;
    public final SJaqlParser.genericOperator_return genericOperator() throws RecognitionException {
        genericOperator_stack.push(new genericOperator_scope());
        SJaqlParser.genericOperator_return retval = new SJaqlParser.genericOperator_return();
        retval.start = input.LT(1);
        int genericOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal87=null;
        SJaqlParser.input_return input86 = null;

        SJaqlParser.input_return input88 = null;

        SJaqlParser.operatorOption_return operatorOption89 = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal87_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_operatorOption=new RewriteRuleSubtreeStream(adaptor,"rule operatorOption");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 26) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:267:2: (name= ID input ( ',' input )* ( operatorOption )* ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:267:4: name= ID input ( ',' input )* ( operatorOption )*
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1275); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            pushFollow(FOLLOW_input_in_genericOperator1277);
            input86=input();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_input.add(input86.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:267:18: ( ',' input )*
            loop36:
            do {
                int alt36=2;
                int LA36_0 = input.LA(1);

                if ( (LA36_0==49) ) {
                    int LA36_2 = input.LA(2);

                    if ( (LA36_2==VAR) ) {
                        int LA36_3 = input.LA(3);

                        if ( (synpred56_SJaql()) ) {
                            alt36=1;
                        }


                    }


                }


                switch (alt36) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:267:19: ',' input
            	    {
            	    char_literal87=(Token)match(input,49,FOLLOW_49_in_genericOperator1280); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_49.add(char_literal87);

            	    pushFollow(FOLLOW_input_in_genericOperator1282);
            	    input88=input();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_input.add(input88.getTree());

            	    }
            	    break;

            	default :
            	    break loop36;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               
                OperatorFactory.OperatorInfo info = operatorFactory.getOperatorInfo((name!=null?name.getText():null));
                if(info == null)
                  throw new IllegalArgumentException("Unknown operator:", new RecognitionException(name.getInputStream()));
                ((operator_scope)operator_stack.peek()).result = info.newInstance(((operator_scope)operator_stack.peek()).inputOperators);
                if(((operator_scope)operator_stack.peek()).aliasNames.size() == 1 && ((operator_scope)operator_stack.peek()).aliasNames.get(0).equals("$0"))
                  ((operator_scope)operator_stack.peek()).aliasNames.set(0, "$");
                ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = info;

            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:276:3: ( operatorOption )*
            loop37:
            do {
                int alt37=2;
                int LA37_0 = input.LA(1);

                if ( (LA37_0==ID) ) {
                    int LA37_2 = input.LA(2);

                    if ( (synpred57_SJaql()) ) {
                        alt37=1;
                    }


                }


                switch (alt37) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: operatorOption
            	    {
            	    pushFollow(FOLLOW_operatorOption_in_genericOperator1289);
            	    operatorOption89=operatorOption();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorOption.add(operatorOption89.getTree());

            	    }
            	    break;

            	default :
            	    break loop37;
                }
            } while (true);



            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 276:19: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 26, genericOperator_StartIndex); }
            genericOperator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "genericOperator"

    public static class operatorOption_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorOption"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:278:1: operatorOption : (name= ID )+ expr= expression ->;
    public final SJaqlParser.operatorOption_return operatorOption() throws RecognitionException {
        SJaqlParser.operatorOption_return retval = new SJaqlParser.operatorOption_return();
        retval.start = input.LT(1);
        int operatorOption_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        SJaqlParser.expression_return expr = null;


        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 27) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:2: ( (name= ID )+ expr= expression ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:4: (name= ID )+ expr= expression
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:8: (name= ID )+
            int cnt38=0;
            loop38:
            do {
                int alt38=2;
                alt38 = dfa38.predict(input);
                switch (alt38) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: name= ID
            	    {
            	    name=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1305); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(name);


            	    }
            	    break;

            	default :
            	    if ( cnt38 >= 1 ) break loop38;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(38, input);
                        throw eee;
                }
                cnt38++;
            } while (true);

            pushFollow(FOLLOW_expression_in_operatorOption1310);
            expr=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expr.getTree());
            if ( state.backtracking==0 ) {
               ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.setProperty((name!=null?name.getText():null), (expr!=null?((EvaluationExpression)expr.tree):null)); 
            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 279:101: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 27, operatorOption_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "operatorOption"

    public static class input_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "input"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:1: input : (name= VAR 'in' )? from= VAR ->;
    public final SJaqlParser.input_return input() throws RecognitionException {
        SJaqlParser.input_return retval = new SJaqlParser.input_return();
        retval.start = input.LT(1);
        int input_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token from=null;
        Token string_literal90=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression string_literal90_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 28) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:2: ( (name= VAR 'in' )? from= VAR ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:4: (name= VAR 'in' )? from= VAR
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:4: (name= VAR 'in' )?
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==VAR) ) {
                int LA39_1 = input.LA(2);

                if ( (LA39_1==31) ) {
                    int LA39_2 = input.LA(3);

                    if ( (LA39_2==VAR) ) {
                        int LA39_4 = input.LA(4);

                        if ( (synpred59_SJaql()) ) {
                            alt39=1;
                        }
                    }
                }
            }
            switch (alt39) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:5: name= VAR 'in'
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input1327); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);

                    string_literal90=(Token)match(input,31,FOLLOW_31_in_input1329); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(string_literal90);


                    }
                    break;

            }

            from=(Token)match(input,VAR,FOLLOW_VAR_in_input1335); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
              	((operator_scope)operator_stack.peek()).inputOperators.add(getVariable(from)); 
                ((operator_scope)operator_stack.peek()).inputNames.add(from.getText()); 
                ((operator_scope)operator_stack.peek()).aliasNames.add(name == null ? String.format("$%d", ((operator_scope)operator_stack.peek()).aliasNames.size()) : name.getText()); 

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 287:3: ->
            {
                root_0 = null;
            }

            retval.tree = root_0;}
            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 28, input_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "input"

    // $ANTLR start synpred4_SJaql
    public final void synpred4_SJaql_fragment() throws RecognitionException {   
        List list_exprs=null;
        RuleReturnScope exprs = null;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:27: ( ( 'or' | '||' ) exprs+= andExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:27: ( 'or' | '||' ) exprs+= andExpression
        {
        if ( (input.LA(1)>=27 && input.LA(1)<=28) ) {
            input.consume();
            state.errorRecovery=false;state.failed=false;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            MismatchedSetException mse = new MismatchedSetException(null,input);
            throw mse;
        }

        pushFollow(FOLLOW_andExpression_in_synpred4_SJaql211);
        exprs=andExpression();

        state._fsp--;
        if (state.failed) return ;
        if (list_exprs==null) list_exprs=new ArrayList();
        list_exprs.add(exprs);


        }
    }
    // $ANTLR end synpred4_SJaql

    // $ANTLR start synpred6_SJaql
    public final void synpred6_SJaql_fragment() throws RecognitionException {   
        List list_exprs=null;
        RuleReturnScope exprs = null;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:31: ( ( 'and' | '&&' ) exprs+= elementExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:31: ( 'and' | '&&' ) exprs+= elementExpression
        {
        if ( (input.LA(1)>=29 && input.LA(1)<=30) ) {
            input.consume();
            state.errorRecovery=false;state.failed=false;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            MismatchedSetException mse = new MismatchedSetException(null,input);
            throw mse;
        }

        pushFollow(FOLLOW_elementExpression_in_synpred6_SJaql258);
        exprs=elementExpression();

        state._fsp--;
        if (state.failed) return ;
        if (list_exprs==null) list_exprs=new ArrayList();
        list_exprs.add(exprs);


        }
    }
    // $ANTLR end synpred6_SJaql

    // $ANTLR start synpred7_SJaql
    public final void synpred7_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:4: ( comparisonExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:4: comparisonExpression
        {
        pushFollow(FOLLOW_comparisonExpression_in_synpred7_SJaql290);
        comparisonExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred7_SJaql

    // $ANTLR start synpred8_SJaql
    public final void synpred8_SJaql_fragment() throws RecognitionException {   
        SJaqlParser.comparisonExpression_return elem = null;

        SJaqlParser.comparisonExpression_return set = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:146:4: (elem= comparisonExpression 'in' set= comparisonExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:146:4: elem= comparisonExpression 'in' set= comparisonExpression
        {
        pushFollow(FOLLOW_comparisonExpression_in_synpred8_SJaql297);
        elem=comparisonExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,31,FOLLOW_31_in_synpred8_SJaql299); if (state.failed) return ;
        pushFollow(FOLLOW_comparisonExpression_in_synpred8_SJaql303);
        set=comparisonExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred8_SJaql

    // $ANTLR start synpred14_SJaql
    public final void synpred14_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.arithmeticExpression_return e2 = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:29: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
        int alt40=6;
        switch ( input.LA(1) ) {
        case 33:
            {
            alt40=1;
            }
            break;
        case 34:
            {
            alt40=2;
            }
            break;
        case 35:
            {
            alt40=3;
            }
            break;
        case 36:
            {
            alt40=4;
            }
            break;
        case 37:
            {
            alt40=5;
            }
            break;
        case 38:
            {
            alt40=6;
            }
            break;
        default:
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 40, 0, input);

            throw nvae;
        }

        switch (alt40) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:30: s= '<='
                {
                s=(Token)match(input,33,FOLLOW_33_in_synpred14_SJaql364); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:39: s= '>='
                {
                s=(Token)match(input,34,FOLLOW_34_in_synpred14_SJaql370); if (state.failed) return ;

                }
                break;
            case 3 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:48: s= '<'
                {
                s=(Token)match(input,35,FOLLOW_35_in_synpred14_SJaql376); if (state.failed) return ;

                }
                break;
            case 4 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:56: s= '>'
                {
                s=(Token)match(input,36,FOLLOW_36_in_synpred14_SJaql382); if (state.failed) return ;

                }
                break;
            case 5 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:64: s= '=='
                {
                s=(Token)match(input,37,FOLLOW_37_in_synpred14_SJaql388); if (state.failed) return ;

                }
                break;
            case 6 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:73: s= '!='
                {
                s=(Token)match(input,38,FOLLOW_38_in_synpred14_SJaql394); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_arithmeticExpression_in_synpred14_SJaql399);
        e2=arithmeticExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred14_SJaql

    // $ANTLR start synpred16_SJaql
    public final void synpred16_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.multiplicationExpression_return e2 = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: ( (s= '+' | s= '-' ) e2= multiplicationExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' ) e2= multiplicationExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:33: (s= '+' | s= '-' )
        int alt41=2;
        int LA41_0 = input.LA(1);

        if ( (LA41_0==39) ) {
            alt41=1;
        }
        else if ( (LA41_0==40) ) {
            alt41=2;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 41, 0, input);

            throw nvae;
        }
        switch (alt41) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:34: s= '+'
                {
                s=(Token)match(input,39,FOLLOW_39_in_synpred16_SJaql463); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:42: s= '-'
                {
                s=(Token)match(input,40,FOLLOW_40_in_synpred16_SJaql469); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_multiplicationExpression_in_synpred16_SJaql474);
        e2=multiplicationExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred16_SJaql

    // $ANTLR start synpred18_SJaql
    public final void synpred18_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.preincrementExpression_return e2 = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: ( (s= '*' | s= '/' ) e2= preincrementExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' ) e2= preincrementExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:31: (s= '*' | s= '/' )
        int alt42=2;
        int LA42_0 = input.LA(1);

        if ( (LA42_0==STAR) ) {
            alt42=1;
        }
        else if ( (LA42_0==41) ) {
            alt42=2;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 42, 0, input);

            throw nvae;
        }
        switch (alt42) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:32: s= '*'
                {
                s=(Token)match(input,STAR,FOLLOW_STAR_in_synpred18_SJaql523); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:40: s= '/'
                {
                s=(Token)match(input,41,FOLLOW_41_in_synpred18_SJaql529); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_preincrementExpression_in_synpred18_SJaql534);
        e2=preincrementExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred18_SJaql

    // $ANTLR start synpred23_SJaql
    public final void synpred23_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( ( '.' ( ID ) ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( '.' ( ID ) )
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( '.' ( ID ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:6: '.' ( ID )
        {
        match(input,46,FOLLOW_46_in_synpred23_SJaql640); if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:10: ( ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:11: ID
        {
        match(input,ID,FOLLOW_ID_in_synpred23_SJaql643); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred23_SJaql

    // $ANTLR start synpred24_SJaql
    public final void synpred24_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( arrayAccess )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred24_SJaql667);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred24_SJaql

    // $ANTLR start synpred25_SJaql
    public final void synpred25_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:5: ( valueExpression ( ( '.' ( ID ) ) | arrayAccess )+ )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:5: valueExpression ( ( '.' ( ID ) ) | arrayAccess )+
        {
        pushFollow(FOLLOW_valueExpression_in_synpred25_SJaql631);
        valueExpression();

        state._fsp--;
        if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:21: ( ( '.' ( ID ) ) | arrayAccess )+
        int cnt43=0;
        loop43:
        do {
            int alt43=3;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==46) ) {
                alt43=1;
            }
            else if ( (LA43_0==55) ) {
                alt43=2;
            }


            switch (alt43) {
        	case 1 :
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( '.' ( ID ) )
        	    {
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( '.' ( ID ) )
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:6: '.' ( ID )
        	    {
        	    match(input,46,FOLLOW_46_in_synpred25_SJaql640); if (state.failed) return ;
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:10: ( ID )
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:11: ID
        	    {
        	    match(input,ID,FOLLOW_ID_in_synpred25_SJaql643); if (state.failed) return ;

        	    }


        	    }


        	    }
        	    break;
        	case 2 :
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: arrayAccess
        	    {
        	    pushFollow(FOLLOW_arrayAccess_in_synpred25_SJaql667);
        	    arrayAccess();

        	    state._fsp--;
        	    if (state.failed) return ;

        	    }
        	    break;

        	default :
        	    if ( cnt43 >= 1 ) break loop43;
        	    if (state.backtracking>0) {state.failed=true; return ;}
                    EarlyExitException eee =
                        new EarlyExitException(43, input);
                    throw eee;
            }
            cnt43++;
        } while (true);


        }
    }
    // $ANTLR end synpred25_SJaql

    // $ANTLR start synpred56_SJaql
    public final void synpred56_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:267:19: ( ',' input )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:267:19: ',' input
        {
        match(input,49,FOLLOW_49_in_synpred56_SJaql1280); if (state.failed) return ;
        pushFollow(FOLLOW_input_in_synpred56_SJaql1282);
        input();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred56_SJaql

    // $ANTLR start synpred57_SJaql
    public final void synpred57_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:276:3: ( operatorOption )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:276:3: operatorOption
        {
        pushFollow(FOLLOW_operatorOption_in_synpred57_SJaql1289);
        operatorOption();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred57_SJaql

    // $ANTLR start synpred58_SJaql
    public final void synpred58_SJaql_fragment() throws RecognitionException {   
        Token name=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:8: (name= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:279:8: name= ID
        {
        name=(Token)match(input,ID,FOLLOW_ID_in_synpred58_SJaql1305); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred58_SJaql

    // $ANTLR start synpred59_SJaql
    public final void synpred59_SJaql_fragment() throws RecognitionException {   
        Token name=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:5: (name= VAR 'in' )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:282:5: name= VAR 'in'
        {
        name=(Token)match(input,VAR,FOLLOW_VAR_in_synpred59_SJaql1327); if (state.failed) return ;
        match(input,31,FOLLOW_31_in_synpred59_SJaql1329); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred59_SJaql

    // Delegated rules

    public final boolean synpred14_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred14_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred7_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred7_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred59_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred59_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred24_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred24_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred16_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred16_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred23_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred23_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred25_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred25_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred8_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred8_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred18_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred18_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred57_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred57_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred58_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred58_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred56_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred56_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred4_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred4_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred6_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred6_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA7 dfa7 = new DFA7(this);
    protected DFA17 dfa17 = new DFA17(this);
    protected DFA38 dfa38 = new DFA38(this);
    static final String DFA7_eotS =
        "\24\uffff";
    static final String DFA7_eofS =
        "\24\uffff";
    static final String DFA7_minS =
        "\1\6\20\0\3\uffff";
    static final String DFA7_maxS =
        "\1\72\20\0\3\uffff";
    static final String DFA7_acceptS =
        "\21\uffff\1\1\1\2\1\3";
    static final String DFA7_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\1\16\1\17\3\uffff}>";
    static final String[] DFA7_transitionS = {
            "\1\14\1\4\1\uffff\1\10\1\11\1\12\1\13\35\uffff\1\1\1\2\2\3\1"+
            "\uffff\1\5\3\uffff\1\16\1\uffff\1\6\1\7\1\15\1\uffff\1\17\1"+
            "\20",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            "",
            ""
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
            return "144:1: elementExpression : ( comparisonExpression | elem= comparisonExpression 'in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) | elem= comparisonExpression 'not in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA7_1 = input.LA(1);

                         
                        int index7_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA7_2 = input.LA(1);

                         
                        int index7_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA7_3 = input.LA(1);

                         
                        int index7_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA7_4 = input.LA(1);

                         
                        int index7_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA7_5 = input.LA(1);

                         
                        int index7_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA7_6 = input.LA(1);

                         
                        int index7_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA7_7 = input.LA(1);

                         
                        int index7_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA7_8 = input.LA(1);

                         
                        int index7_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA7_9 = input.LA(1);

                         
                        int index7_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA7_10 = input.LA(1);

                         
                        int index7_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA7_11 = input.LA(1);

                         
                        int index7_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA7_12 = input.LA(1);

                         
                        int index7_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA7_13 = input.LA(1);

                         
                        int index7_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA7_14 = input.LA(1);

                         
                        int index7_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_14);
                        if ( s>=0 ) return s;
                        break;
                    case 14 : 
                        int LA7_15 = input.LA(1);

                         
                        int index7_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_15);
                        if ( s>=0 ) return s;
                        break;
                    case 15 : 
                        int LA7_16 = input.LA(1);

                         
                        int index7_16 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred7_SJaql()) ) {s = 17;}

                        else if ( (synpred8_SJaql()) ) {s = 18;}

                        else if ( (true) ) {s = 19;}

                         
                        input.seek(index7_16);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 7, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA17_eotS =
        "\20\uffff";
    static final String DFA17_eofS =
        "\20\uffff";
    static final String DFA17_minS =
        "\1\6\15\0\2\uffff";
    static final String DFA17_maxS =
        "\1\72\15\0\2\uffff";
    static final String DFA17_acceptS =
        "\16\uffff\1\1\1\2";
    static final String DFA17_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\2\uffff}>";
    static final String[] DFA17_transitionS = {
            "\1\11\1\1\1\uffff\1\5\1\6\1\7\1\10\42\uffff\1\2\3\uffff\1\13"+
            "\1\uffff\1\3\1\4\1\12\1\uffff\1\14\1\15",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "\1\uffff",
            "",
            ""
    };

    static final short[] DFA17_eot = DFA.unpackEncodedString(DFA17_eotS);
    static final short[] DFA17_eof = DFA.unpackEncodedString(DFA17_eofS);
    static final char[] DFA17_min = DFA.unpackEncodedStringToUnsignedChars(DFA17_minS);
    static final char[] DFA17_max = DFA.unpackEncodedStringToUnsignedChars(DFA17_maxS);
    static final short[] DFA17_accept = DFA.unpackEncodedString(DFA17_acceptS);
    static final short[] DFA17_special = DFA.unpackEncodedString(DFA17_specialS);
    static final short[][] DFA17_transition;

    static {
        int numStates = DFA17_transitionS.length;
        DFA17_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA17_transition[i] = DFA.unpackEncodedString(DFA17_transitionS[i]);
        }
    }

    class DFA17 extends DFA {

        public DFA17(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 17;
            this.eot = DFA17_eot;
            this.eof = DFA17_eof;
            this.min = DFA17_min;
            this.max = DFA17_max;
            this.accept = DFA17_accept;
            this.special = DFA17_special;
            this.transition = DFA17_transition;
        }
        public String getDescription() {
            return "178:1: pathExpression : ( valueExpression ( ( '.' ( ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA17_1 = input.LA(1);

                         
                        int index17_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA17_2 = input.LA(1);

                         
                        int index17_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA17_3 = input.LA(1);

                         
                        int index17_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA17_4 = input.LA(1);

                         
                        int index17_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA17_5 = input.LA(1);

                         
                        int index17_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA17_6 = input.LA(1);

                         
                        int index17_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA17_7 = input.LA(1);

                         
                        int index17_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA17_8 = input.LA(1);

                         
                        int index17_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA17_9 = input.LA(1);

                         
                        int index17_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA17_10 = input.LA(1);

                         
                        int index17_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA17_11 = input.LA(1);

                         
                        int index17_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA17_12 = input.LA(1);

                         
                        int index17_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA17_13 = input.LA(1);

                         
                        int index17_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index17_13);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 17, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA38_eotS =
        "\22\uffff";
    static final String DFA38_eofS =
        "\22\uffff";
    static final String DFA38_minS =
        "\1\6\3\uffff\1\0\15\uffff";
    static final String DFA38_maxS =
        "\1\72\3\uffff\1\0\15\uffff";
    static final String DFA38_acceptS =
        "\1\uffff\1\2\17\uffff\1\1";
    static final String DFA38_specialS =
        "\4\uffff\1\0\15\uffff}>";
    static final String[] DFA38_transitionS = {
            "\1\1\1\4\1\uffff\4\1\35\uffff\4\1\1\uffff\1\1\3\uffff\1\1\1"+
            "\uffff\3\1\1\uffff\2\1",
            "",
            "",
            "",
            "\1\uffff",
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
            ""
    };

    static final short[] DFA38_eot = DFA.unpackEncodedString(DFA38_eotS);
    static final short[] DFA38_eof = DFA.unpackEncodedString(DFA38_eofS);
    static final char[] DFA38_min = DFA.unpackEncodedStringToUnsignedChars(DFA38_minS);
    static final char[] DFA38_max = DFA.unpackEncodedStringToUnsignedChars(DFA38_maxS);
    static final short[] DFA38_accept = DFA.unpackEncodedString(DFA38_acceptS);
    static final short[] DFA38_special = DFA.unpackEncodedString(DFA38_specialS);
    static final short[][] DFA38_transition;

    static {
        int numStates = DFA38_transitionS.length;
        DFA38_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA38_transition[i] = DFA.unpackEncodedString(DFA38_transitionS[i]);
        }
    }

    class DFA38 extends DFA {

        public DFA38(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 38;
            this.eot = DFA38_eot;
            this.eof = DFA38_eof;
            this.min = DFA38_min;
            this.max = DFA38_max;
            this.accept = DFA38_accept;
            this.special = DFA38_special;
            this.transition = DFA38_transition;
        }
        public String getDescription() {
            return "()+ loopback of 279:8: (name= ID )+";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA38_4 = input.LA(1);

                         
                        int index38_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred58_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index38_4);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 38, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_statement_in_script125 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_25_in_script128 = new BitSet(new long[]{0x06000000000000C0L});
    public static final BitSet FOLLOW_statement_in_script130 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_25_in_script134 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assignment_in_statement146 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_statement150 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_assignment166 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_assignment168 = new BitSet(new long[]{0x06000000000000C0L});
    public static final BitSet FOLLOW_operator_in_assignment172 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_expression186 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression198 = new BitSet(new long[]{0x0000000018000002L});
    public static final BitSet FOLLOW_27_in_orExpression202 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_28_in_orExpression206 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_andExpression_in_orExpression211 = new BitSet(new long[]{0x0000000018000002L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression245 = new BitSet(new long[]{0x0000000060000002L});
    public static final BitSet FOLLOW_29_in_andExpression249 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_30_in_andExpression253 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression258 = new BitSet(new long[]{0x0000000060000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression290 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression297 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_elementExpression299 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression303 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression325 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_elementExpression327 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression331 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression358 = new BitSet(new long[]{0x0000007E00000002L});
    public static final BitSet FOLLOW_33_in_comparisonExpression364 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_34_in_comparisonExpression370 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_35_in_comparisonExpression376 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_36_in_comparisonExpression382 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_37_in_comparisonExpression388 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_38_in_comparisonExpression394 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression399 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression457 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_39_in_arithmeticExpression463 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_40_in_arithmeticExpression469 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression474 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression517 = new BitSet(new long[]{0x0000020000000102L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression523 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_41_in_multiplicationExpression529 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression534 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_42_in_preincrementExpression575 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression577 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_43_in_preincrementExpression582 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression584 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression589 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unaryExpression599 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_pathExpression_in_unaryExpression608 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression631 = new BitSet(new long[]{0x0080400000000000L});
    public static final BitSet FOLLOW_46_in_pathExpression640 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_pathExpression643 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathExpression667 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression693 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_valueExpression702 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression708 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression714 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression720 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression729 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression735 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_valueExpression741 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression751 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_47_in_parenthesesExpression763 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression765 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_parenthesesExpression767 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionCall781 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_functionCall783 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_functionCall788 = new BitSet(new long[]{0x0003000000000000L});
    public static final BitSet FOLLOW_49_in_functionCall791 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_functionCall795 = new BitSet(new long[]{0x0003000000000000L});
    public static final BitSet FOLLOW_48_in_functionCall800 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment824 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_fieldAssignment826 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment828 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment837 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_fieldAssignment839 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment841 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment850 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_fieldAssignment852 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_fieldAssignment854 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_51_in_objectCreation876 = new BitSet(new long[]{0x00100000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation879 = new BitSet(new long[]{0x0012000000000000L});
    public static final BitSet FOLLOW_49_in_objectCreation882 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation884 = new BitSet(new long[]{0x0012000000000000L});
    public static final BitSet FOLLOW_49_in_objectCreation888 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_objectCreation893 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_53_in_literal917 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_54_in_literal933 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal949 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal965 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal982 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UINT_in_literal999 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayAccess1019 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1024 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1028 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayAccess1031 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayAccess1047 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1052 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1056 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_arrayAccess1059 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1064 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1068 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayAccess1071 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayAccess1089 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1091 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayAccess1093 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayCreation1111 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1115 = new BitSet(new long[]{0x0102000000000000L});
    public static final BitSet FOLLOW_49_in_arrayCreation1118 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1122 = new BitSet(new long[]{0x0102000000000000L});
    public static final BitSet FOLLOW_49_in_arrayCreation1126 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayCreation1129 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator1164 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator1168 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator1172 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_readOperator1186 = new BitSet(new long[]{0x0000000000000480L});
    public static final BitSet FOLLOW_ID_in_readOperator1191 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_readOperator1196 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_readOperator1202 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_readOperator1204 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_readOperator1208 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_readOperator1210 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_writeOperator1224 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_VAR_in_writeOperator1228 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_writeOperator1230 = new BitSet(new long[]{0x0000000000000480L});
    public static final BitSet FOLLOW_ID_in_writeOperator1235 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1240 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_writeOperator1246 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_writeOperator1248 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1252 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_writeOperator1254 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_genericOperator1275 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_input_in_genericOperator1277 = new BitSet(new long[]{0x0002000000000082L});
    public static final BitSet FOLLOW_49_in_genericOperator1280 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_input_in_genericOperator1282 = new BitSet(new long[]{0x0002000000000082L});
    public static final BitSet FOLLOW_operatorOption_in_genericOperator1289 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_ID_in_operatorOption1305 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_operatorOption1310 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_input1327 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_input1329 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_VAR_in_input1335 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred4_SJaql201 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_andExpression_in_synpred4_SJaql211 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred6_SJaql248 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_elementExpression_in_synpred6_SJaql258 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred7_SJaql290 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred8_SJaql297 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_synpred8_SJaql299 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred8_SJaql303 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_33_in_synpred14_SJaql364 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_34_in_synpred14_SJaql370 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_35_in_synpred14_SJaql376 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_36_in_synpred14_SJaql382 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_37_in_synpred14_SJaql388 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_38_in_synpred14_SJaql394 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_synpred14_SJaql399 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_39_in_synpred16_SJaql463 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_40_in_synpred16_SJaql469 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_synpred16_SJaql474 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_synpred18_SJaql523 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_41_in_synpred18_SJaql529 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_synpred18_SJaql534 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_46_in_synpred23_SJaql640 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_synpred23_SJaql643 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred24_SJaql667 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_synpred25_SJaql631 = new BitSet(new long[]{0x0080400000000000L});
    public static final BitSet FOLLOW_46_in_synpred25_SJaql640 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_synpred25_SJaql643 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred25_SJaql667 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_49_in_synpred56_SJaql1280 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_input_in_synpred56_SJaql1282 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorOption_in_synpred57_SJaql1289 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred58_SJaql1305 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred59_SJaql1327 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_synpred59_SJaql1329 = new BitSet(new long[]{0x0000000000000002L});

}