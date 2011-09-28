// $ANTLR 3.3 Nov 30, 2010 12:46:29 /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g 2011-09-26 12:43:02
 
package eu.stratosphere.simple.jaql; 

import eu.stratosphere.sopremo.*;
import eu.stratosphere.util.*;
import eu.stratosphere.simple.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;
import java.util.Arrays;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;

public class SJaqlParser extends SimpleParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "EXPRESSION", "OPERATOR", "VAR", "ID", "STAR", "DECIMAL", "STRING", "INTEGER", "UINT", "LOWER_LETTER", "UPPER_LETTER", "DIGIT", "SIGN", "COMMENT", "QUOTE", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'='", "'or'", "'||'", "'and'", "'&&'", "'in'", "'not in'", "'<='", "'>='", "'<'", "'>'", "'=='", "'!='", "'+'", "'-'", "'/'", "'++'", "'--'", "'!'", "'~'", "'.'", "'('", "')'", "','", "':'", "'{'", "'}'", "'true'", "'false'", "'['", "']'", "'read'", "'write'", "'to'", "'preserve'"
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
    public static final int T__60=60;
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
            this.state.ruleMemo = new HashMap[99+1];
             
             
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

    public void parseSinks() throws RecognitionException {  
        script();
    }

    private EvaluationExpression makePath(Token inputVar, String... path) {
      List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();

      int inputIndex = inputIndexForVariable(inputVar);
      if(inputIndex == -1) {
        accesses.add(new JsonStreamExpression(getVariable(inputVar)));
      } else {
        InputSelection inputSelection = new InputSelection(inputIndex);
        for (ExpressionTag tag : ((operator_scope)operator_stack.peek()).inputTags.get(inputIndex))
          inputSelection.addTag(tag);
        accesses.add(inputSelection);
      }

      for (String fragment : path)
        accesses.add(new ObjectAccess(fragment));

      return PathExpression.valueOf(accesses);
    }

    private Operator getVariable(Token variable) {
    	Operator op = variables.get(variable.getText());
    	if(op == null)
    		throw new IllegalArgumentException("Unknown variable" + variable.getText(), new RecognitionException(variable.getInputStream()));
    	return op;
    }

    private int inputIndexForVariable(Token variable) {
      int index = ((operator_scope)operator_stack.peek()).inputNames.indexOf(variable.getText());
      if(index == -1) {
        if(variable.getText().equals("$") && ((operator_scope)operator_stack.peek()).inputNames.size() == 1 && !((operator_scope)operator_stack.peek()).hasExplicitName.get(0))
          return 0;
        try {
          index = Integer.parseInt(variable.getText().substring(1));
          if(((operator_scope)operator_stack.peek()).hasExplicitName.get(index))
            throw new IllegalArgumentException("Cannot use index variable " + variable.getText() + " for input with explicit name", 
              new RecognitionException(variable.getInputStream()));
          if(0 > index || index >= ((operator_scope)operator_stack.peek()).inputNames.size()) 
            throw new IllegalArgumentException("Invalid input index " + index, new RecognitionException(variable.getInputStream()));
        } catch(NumberFormatException e) {
        }
      }
      return index;
    }

    //private EvaluationExpression[] getExpressions(List nodes


    public static class script_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "script"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:108:1: script : statement ( ';' statement )* ';' ->;
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
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:109:2: ( statement ( ';' statement )* ';' ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:109:5: statement ( ';' statement )* ';'
            {
            pushFollow(FOLLOW_statement_in_script134);
            statement1=statement();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:109:15: ( ';' statement )*
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
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:109:16: ';' statement
            	    {
            	    char_literal2=(Token)match(input,25,FOLLOW_25_in_script137); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_25.add(char_literal2);

            	    pushFollow(FOLLOW_statement_in_script139);
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

            char_literal4=(Token)match(input,25,FOLLOW_25_in_script143); if (state.failed) return retval; 
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
            // 109:36: ->
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:111:1: statement : ( assignment | operator ) ->;
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
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:2: ( ( assignment | operator ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:4: ( assignment | operator )
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:4: ( assignment | operator )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:5: assignment
                    {
                    pushFollow(FOLLOW_assignment_in_statement155);
                    assignment5=assignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_assignment.add(assignment5.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:18: operator
                    {
                    pushFollow(FOLLOW_operator_in_statement159);
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
            // 112:28: ->
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:114:1: assignment : target= VAR '=' source= operator ->;
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
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:115:2: (target= VAR '=' source= operator ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:115:4: target= VAR '=' source= operator
            {
            target=(Token)match(input,VAR,FOLLOW_VAR_in_assignment175); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(target);

            char_literal7=(Token)match(input,26,FOLLOW_26_in_assignment177); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_26.add(char_literal7);

            pushFollow(FOLLOW_operator_in_assignment181);
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
            // 115:80: ->
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

    protected static class contextAwareExpression_scope {
        EvaluationExpression context;
    }
    protected Stack contextAwareExpression_stack = new Stack();

    public static class contextAwareExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwareExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:117:1: contextAwareExpression[EvaluationExpression contextExpression] : expression ;
    public final SJaqlParser.contextAwareExpression_return contextAwareExpression(EvaluationExpression contextExpression) throws RecognitionException {
        contextAwareExpression_stack.push(new contextAwareExpression_scope());
        SJaqlParser.contextAwareExpression_return retval = new SJaqlParser.contextAwareExpression_return();
        retval.start = input.LT(1);
        int contextAwareExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.expression_return expression8 = null;



         ((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context = contextExpression; 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:120:3: ( expression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:120:5: expression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_expression_in_contextAwareExpression207);
            expression8=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression8.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 4, contextAwareExpression_StartIndex); }
            contextAwareExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "contextAwareExpression"

    public static class expression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "expression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:122:1: expression : orExpression ;
    public final SJaqlParser.expression_return expression() throws RecognitionException {
        SJaqlParser.expression_return retval = new SJaqlParser.expression_return();
        retval.start = input.LT(1);
        int expression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.orExpression_return orExpression9 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 5) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:2: ( orExpression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:4: orExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_orExpression_in_expression216);
            orExpression9=orExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, orExpression9.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 5, expression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "expression"

    public static class orExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:125:1: orExpression : exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) ;
    public final SJaqlParser.orExpression_return orExpression() throws RecognitionException {
        SJaqlParser.orExpression_return retval = new SJaqlParser.orExpression_return();
        retval.start = input.LT(1);
        int orExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal10=null;
        Token string_literal11=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal10_tree=null;
        EvaluationExpression string_literal11_tree=null;
        RewriteRuleTokenStream stream_27=new RewriteRuleTokenStream(adaptor,"token 27");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_andExpression=new RewriteRuleSubtreeStream(adaptor,"rule andExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:3: (exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:5: exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression229);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:26: ( ( 'or' | '||' ) exprs+= andExpression )*
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
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:27: ( 'or' | '||' ) exprs+= andExpression
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:27: ( 'or' | '||' )
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
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:28: 'or'
            	            {
            	            string_literal10=(Token)match(input,27,FOLLOW_27_in_orExpression233); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_27.add(string_literal10);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:35: '||'
            	            {
            	            string_literal11=(Token)match(input,28,FOLLOW_28_in_orExpression237); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_28.add(string_literal11);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_andExpression_in_orExpression242);
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
            // 127:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 128:3: -> ^( EXPRESSION[\"OrExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:6: ^( EXPRESSION[\"OrExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 6, orExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "orExpression"

    public static class andExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "andExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:130:1: andExpression : exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) ;
    public final SJaqlParser.andExpression_return andExpression() throws RecognitionException {
        SJaqlParser.andExpression_return retval = new SJaqlParser.andExpression_return();
        retval.start = input.LT(1);
        int andExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal12=null;
        Token string_literal13=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal12_tree=null;
        EvaluationExpression string_literal13_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:3: (exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:5: exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression276);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:30: ( ( 'and' | '&&' ) exprs+= elementExpression )*
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
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:31: ( 'and' | '&&' ) exprs+= elementExpression
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:31: ( 'and' | '&&' )
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
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:32: 'and'
            	            {
            	            string_literal12=(Token)match(input,29,FOLLOW_29_in_andExpression280); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_29.add(string_literal12);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:40: '&&'
            	            {
            	            string_literal13=(Token)match(input,30,FOLLOW_30_in_andExpression284); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_30.add(string_literal13);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_elementExpression_in_andExpression289);
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
            // 132:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 133:3: -> ^( EXPRESSION[\"AndExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:6: ^( EXPRESSION[\"AndExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 7, andExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "andExpression"

    public static class elementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "elementExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:1: elementExpression : ( comparisonExpression | elem= comparisonExpression 'in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) | elem= comparisonExpression 'not in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) );
    public final SJaqlParser.elementExpression_return elementExpression() throws RecognitionException {
        SJaqlParser.elementExpression_return retval = new SJaqlParser.elementExpression_return();
        retval.start = input.LT(1);
        int elementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal15=null;
        Token string_literal16=null;
        SJaqlParser.comparisonExpression_return elem = null;

        SJaqlParser.comparisonExpression_return set = null;

        SJaqlParser.comparisonExpression_return comparisonExpression14 = null;


        EvaluationExpression string_literal15_tree=null;
        EvaluationExpression string_literal16_tree=null;
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:136:2: ( comparisonExpression | elem= comparisonExpression 'in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) | elem= comparisonExpression 'not in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            int alt7=3;
            alt7 = dfa7.predict(input);
            switch (alt7) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:136:4: comparisonExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression321);
                    comparisonExpression14=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, comparisonExpression14.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:137:4: elem= comparisonExpression 'in' set= comparisonExpression
                    {
                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression328);
                    elem=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
                    string_literal15=(Token)match(input,31,FOLLOW_31_in_elementExpression330); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(string_literal15);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression334);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());


                    // AST REWRITE
                    // elements: set, elem
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
                    // 137:60: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:137:63: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:4: elem= comparisonExpression 'not in' set= comparisonExpression
                    {
                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression356);
                    elem=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
                    string_literal16=(Token)match(input,32,FOLLOW_32_in_elementExpression358); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(string_literal16);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression362);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());


                    // AST REWRITE
                    // elements: set, elem
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
                    // 138:64: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:67: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
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
            if ( state.backtracking>0 ) { memoize(input, 8, elementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "elementExpression"

    public static class comparisonExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 9) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:2: (e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:4: e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression389);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:28: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
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
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:30: s= '<='
                            {
                            s=(Token)match(input,33,FOLLOW_33_in_comparisonExpression395); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_33.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:39: s= '>='
                            {
                            s=(Token)match(input,34,FOLLOW_34_in_comparisonExpression401); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_34.add(s);


                            }
                            break;
                        case 3 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:48: s= '<'
                            {
                            s=(Token)match(input,35,FOLLOW_35_in_comparisonExpression407); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_35.add(s);


                            }
                            break;
                        case 4 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:56: s= '>'
                            {
                            s=(Token)match(input,36,FOLLOW_36_in_comparisonExpression413); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_36.add(s);


                            }
                            break;
                        case 5 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:64: s= '=='
                            {
                            s=(Token)match(input,37,FOLLOW_37_in_comparisonExpression419); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_37.add(s);


                            }
                            break;
                        case 6 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:73: s= '!='
                            {
                            s=(Token)match(input,38,FOLLOW_38_in_comparisonExpression425); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_38.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression430);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e1, e2, e1, e2, e1, e1
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
            // 142:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }
            else // 143:3: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 144:3: -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("==") ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 145:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
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
            if ( state.backtracking>0 ) { memoize(input, 9, comparisonExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "comparisonExpression"

    public static class arithmeticExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arithmeticExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:147:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 10) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:2: (e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:4: e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression510);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:32: ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:33: (s= '+' | s= '-' ) e2= multiplicationExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:33: (s= '+' | s= '-' )
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
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:34: s= '+'
                            {
                            s=(Token)match(input,39,FOLLOW_39_in_arithmeticExpression516); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_39.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:42: s= '-'
                            {
                            s=(Token)match(input,40,FOLLOW_40_in_arithmeticExpression522); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_40.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression527);
                    e2=multiplicationExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multiplicationExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e1, e1
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
            // 149:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 151:2: -> $e1
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
            if ( state.backtracking>0 ) { memoize(input, 10, arithmeticExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arithmeticExpression"

    public static class multiplicationExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiplicationExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:153:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 11) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:2: (e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:4: e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression570);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:30: ( (s= '*' | s= '/' ) e2= preincrementExpression )?
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:31: (s= '*' | s= '/' ) e2= preincrementExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:31: (s= '*' | s= '/' )
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
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression576); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:40: s= '/'
                            {
                            s=(Token)match(input,41,FOLLOW_41_in_multiplicationExpression582); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression587);
                    e2=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_preincrementExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e1, e1
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
            // 155:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:155:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 157:2: -> $e1
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
            if ( state.backtracking>0 ) { memoize(input, 11, multiplicationExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "multiplicationExpression"

    public static class preincrementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "preincrementExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:159:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
    public final SJaqlParser.preincrementExpression_return preincrementExpression() throws RecognitionException {
        SJaqlParser.preincrementExpression_return retval = new SJaqlParser.preincrementExpression_return();
        retval.start = input.LT(1);
        int preincrementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal17=null;
        Token string_literal19=null;
        SJaqlParser.preincrementExpression_return preincrementExpression18 = null;

        SJaqlParser.preincrementExpression_return preincrementExpression20 = null;

        SJaqlParser.unaryExpression_return unaryExpression21 = null;


        EvaluationExpression string_literal17_tree=null;
        EvaluationExpression string_literal19_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:160:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:160:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal17=(Token)match(input,42,FOLLOW_42_in_preincrementExpression628); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal17_tree = (EvaluationExpression)adaptor.create(string_literal17);
                    adaptor.addChild(root_0, string_literal17_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression630);
                    preincrementExpression18=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression18.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:161:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal19=(Token)match(input,43,FOLLOW_43_in_preincrementExpression635); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal19_tree = (EvaluationExpression)adaptor.create(string_literal19);
                    adaptor.addChild(root_0, string_literal19_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression637);
                    preincrementExpression20=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression20.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression642);
                    unaryExpression21=unaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression21.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 12, preincrementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "preincrementExpression"

    public static class unaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:164:1: unaryExpression : ( '!' | '~' )? ( ({...}? contextAwarePathExpression ) | pathExpression ) ;
    public final SJaqlParser.unaryExpression_return unaryExpression() throws RecognitionException {
        SJaqlParser.unaryExpression_return retval = new SJaqlParser.unaryExpression_return();
        retval.start = input.LT(1);
        int unaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token set22=null;
        SJaqlParser.contextAwarePathExpression_return contextAwarePathExpression23 = null;

        SJaqlParser.pathExpression_return pathExpression24 = null;


        EvaluationExpression set22_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 13) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:165:2: ( ( '!' | '~' )? ( ({...}? contextAwarePathExpression ) | pathExpression ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:165:4: ( '!' | '~' )? ( ({...}? contextAwarePathExpression ) | pathExpression )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:165:4: ( '!' | '~' )?
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( ((LA15_0>=44 && LA15_0<=45)) ) {
                alt15=1;
            }
            switch (alt15) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:
                    {
                    set22=(Token)input.LT(1);
                    if ( (input.LA(1)>=44 && input.LA(1)<=45) ) {
                        input.consume();
                        if ( state.backtracking==0 ) adaptor.addChild(root_0, (EvaluationExpression)adaptor.create(set22));
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

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:4: ( ({...}? contextAwarePathExpression ) | pathExpression )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==ID) ) {
                switch ( input.LA(2) ) {
                case VAR:
                case 47:
                case 60:
                    {
                    alt16=2;
                    }
                    break;
                case EOF:
                case STAR:
                case 25:
                case 27:
                case 28:
                case 29:
                case 30:
                case 31:
                case 32:
                case 33:
                case 34:
                case 35:
                case 36:
                case 37:
                case 38:
                case 39:
                case 40:
                case 41:
                case 46:
                case 48:
                case 49:
                case 52:
                case 55:
                case 56:
                    {
                    alt16=1;
                    }
                    break;
                case ID:
                    {
                    switch ( input.LA(3) ) {
                    case ID:
                        {
                        int LA16_5 = input.LA(4);

                        if ( ((synpred23_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))) ) {
                            alt16=1;
                        }
                        else if ( (true) ) {
                            alt16=2;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 16, 5, input);

                            throw nvae;
                        }
                        }
                        break;
                    case DECIMAL:
                    case STRING:
                    case INTEGER:
                    case UINT:
                    case 42:
                    case 43:
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
                        alt16=1;
                        }
                        break;
                    case VAR:
                        {
                        int LA16_6 = input.LA(4);

                        if ( ((synpred23_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))) ) {
                            alt16=1;
                        }
                        else if ( (true) ) {
                            alt16=2;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 16, 6, input);

                            throw nvae;
                        }
                        }
                        break;
                    case 60:
                        {
                        alt16=2;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 16, 4, input);

                        throw nvae;
                    }

                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 16, 1, input);

                    throw nvae;
                }

            }
            else if ( (LA16_0==VAR||(LA16_0>=DECIMAL && LA16_0<=UINT)||LA16_0==47||LA16_0==51||(LA16_0>=53 && LA16_0<=55)||(LA16_0>=57 && LA16_0<=58)) ) {
                alt16=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:5: ({...}? contextAwarePathExpression )
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:5: ({...}? contextAwarePathExpression )
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:6: {...}? contextAwarePathExpression
                    {
                    if ( !((((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "unaryExpression", "$contextAwareExpression::context != null");
                    }
                    pushFollow(FOLLOW_contextAwarePathExpression_in_unaryExpression669);
                    contextAwarePathExpression23=contextAwarePathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, contextAwarePathExpression23.getTree());

                    }


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:80: pathExpression
                    {
                    pushFollow(FOLLOW_pathExpression_in_unaryExpression674);
                    pathExpression24=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, pathExpression24.getTree());

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 13, unaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "unaryExpression"

    protected static class contextAwarePathExpression_scope {
        List<EvaluationExpression> fragments;
    }
    protected Stack contextAwarePathExpression_stack = new Stack();

    public static class contextAwarePathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwarePathExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:171:1: contextAwarePathExpression : start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) ;
    public final SJaqlParser.contextAwarePathExpression_return contextAwarePathExpression() throws RecognitionException {
        contextAwarePathExpression_stack.push(new contextAwarePathExpression_scope());
        SJaqlParser.contextAwarePathExpression_return retval = new SJaqlParser.contextAwarePathExpression_return();
        retval.start = input.LT(1);
        int contextAwarePathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token start=null;
        Token field=null;
        Token char_literal25=null;
        SJaqlParser.arrayAccess_return arrayAccess26 = null;


        EvaluationExpression start_tree=null;
        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal25_tree=null;
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
         ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:174:3: (start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:174:5: start= ID ( ( '.' (field= ID ) ) | arrayAccess )*
            {
            start=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression700); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(start);

            if ( state.backtracking==0 ) {
               ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context); ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((start!=null?start.getText():null)));
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:5: ( ( '.' (field= ID ) ) | arrayAccess )*
            loop17:
            do {
                int alt17=3;
                alt17 = dfa17.predict(input);
                switch (alt17) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:7: ( '.' (field= ID ) )
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:7: ( '.' (field= ID ) )
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:8: '.' (field= ID )
            	    {
            	    char_literal25=(Token)match(input,46,FOLLOW_46_in_contextAwarePathExpression711); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_46.add(char_literal25);

            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:12: (field= ID )
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:13: field= ID
            	    {
            	    field=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression716); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(field);

            	    if ( state.backtracking==0 ) {
            	       ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
            	    }

            	    }


            	    }


            	    }
            	    break;
            	case 2 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:11: arrayAccess
            	    {
            	    pushFollow(FOLLOW_arrayAccess_in_contextAwarePathExpression734);
            	    arrayAccess26=arrayAccess();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess26.getTree());
            	    if ( state.backtracking==0 ) {
            	       ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add((arrayAccess26!=null?((EvaluationExpression)arrayAccess26.tree):null)); 
            	    }

            	    }
            	    break;

            	default :
            	    break loop17;
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
            // 176:93: -> ^( EXPRESSION[\"PathExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:97: ^( EXPRESSION[\"PathExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "PathExpression"), root_1);

                adaptor.addChild(root_1,  ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments );

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
            if ( state.backtracking>0 ) { memoize(input, 14, contextAwarePathExpression_StartIndex); }
            contextAwarePathExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "contextAwarePathExpression"

    protected static class pathExpression_scope {
        List<EvaluationExpression> fragments;
    }
    protected Stack pathExpression_stack = new Stack();

    public static class pathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "pathExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:1: pathExpression : ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );
    public final SJaqlParser.pathExpression_return pathExpression() throws RecognitionException {
        pathExpression_stack.push(new pathExpression_scope());
        SJaqlParser.pathExpression_return retval = new SJaqlParser.pathExpression_return();
        retval.start = input.LT(1);
        int pathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token field=null;
        Token char_literal28=null;
        SJaqlParser.valueExpression_return valueExpression27 = null;

        SJaqlParser.arrayAccess_return arrayAccess29 = null;

        SJaqlParser.valueExpression_return valueExpression30 = null;


        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal28_tree=null;
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
         ((pathExpression_scope)pathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:3: ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression )
            int alt19=2;
            alt19 = dfa19.predict(input);
            switch (alt19) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+
                    {
                    pushFollow(FOLLOW_valueExpression_in_pathExpression776);
                    valueExpression27=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(valueExpression27.getTree());
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:5: ( ( '.' (field= ID ) ) | arrayAccess )+
                    int cnt18=0;
                    loop18:
                    do {
                        int alt18=3;
                        int LA18_0 = input.LA(1);

                        if ( (LA18_0==46) ) {
                            int LA18_2 = input.LA(2);

                            if ( (synpred26_SJaql()) ) {
                                alt18=1;
                            }


                        }
                        else if ( (LA18_0==55) ) {
                            int LA18_3 = input.LA(2);

                            if ( (synpred27_SJaql()) ) {
                                alt18=2;
                            }


                        }


                        switch (alt18) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( '.' (field= ID ) )
                    	    {
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( '.' (field= ID ) )
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:8: '.' (field= ID )
                    	    {
                    	    char_literal28=(Token)match(input,46,FOLLOW_46_in_pathExpression790); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_46.add(char_literal28);

                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:12: (field= ID )
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:13: field= ID
                    	    {
                    	    field=(Token)match(input,ID,FOLLOW_ID_in_pathExpression795); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(field);

                    	    if ( state.backtracking==0 ) {
                    	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
                    	    }

                    	    }


                    	    }


                    	    }
                    	    break;
                    	case 2 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:11: arrayAccess
                    	    {
                    	    pushFollow(FOLLOW_arrayAccess_in_pathExpression813);
                    	    arrayAccess29=arrayAccess();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess29.getTree());
                    	    if ( state.backtracking==0 ) {
                    	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add((arrayAccess29!=null?((EvaluationExpression)arrayAccess29.tree):null)); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    if ( cnt18 >= 1 ) break loop18;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(18, input);
                                throw eee;
                        }
                        cnt18++;
                    } while (true);

                    if ( state.backtracking==0 ) {
                       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(0, (valueExpression27!=null?((EvaluationExpression)valueExpression27.tree):null)); 
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
                    // 185:143: -> ^( EXPRESSION[\"PathExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:147: ^( EXPRESSION[\"PathExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:187:5: valueExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_valueExpression_in_pathExpression840);
                    valueExpression30=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, valueExpression30.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 15, pathExpression_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:1: valueExpression : ( functionCall | parenthesesExpression | literal | VAR -> | arrayCreation | objectCreation | operatorExpression );
    public final SJaqlParser.valueExpression_return valueExpression() throws RecognitionException {
        SJaqlParser.valueExpression_return retval = new SJaqlParser.valueExpression_return();
        retval.start = input.LT(1);
        int valueExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR34=null;
        SJaqlParser.functionCall_return functionCall31 = null;

        SJaqlParser.parenthesesExpression_return parenthesesExpression32 = null;

        SJaqlParser.literal_return literal33 = null;

        SJaqlParser.arrayCreation_return arrayCreation35 = null;

        SJaqlParser.objectCreation_return objectCreation36 = null;

        SJaqlParser.operatorExpression_return operatorExpression37 = null;


        EvaluationExpression VAR34_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 16) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:190:2: ( functionCall | parenthesesExpression | literal | VAR -> | arrayCreation | objectCreation | operatorExpression )
            int alt20=7;
            switch ( input.LA(1) ) {
            case ID:
                {
                int LA20_1 = input.LA(2);

                if ( (LA20_1==47) ) {
                    alt20=1;
                }
                else if ( ((LA20_1>=VAR && LA20_1<=ID)||LA20_1==60) ) {
                    alt20=7;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 20, 1, input);

                    throw nvae;
                }
                }
                break;
            case 47:
                {
                alt20=2;
                }
                break;
            case DECIMAL:
            case STRING:
            case INTEGER:
            case UINT:
            case 53:
            case 54:
                {
                alt20=3;
                }
                break;
            case VAR:
                {
                alt20=4;
                }
                break;
            case 55:
                {
                alt20=5;
                }
                break;
            case 51:
                {
                alt20=6;
                }
                break;
            case 57:
            case 58:
                {
                alt20=7;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 20, 0, input);

                throw nvae;
            }

            switch (alt20) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:190:4: functionCall
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_functionCall_in_valueExpression849);
                    functionCall31=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionCall31.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:191:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression855);
                    parenthesesExpression32=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression32.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_valueExpression861);
                    literal33=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal33.getTree());

                    }
                    break;
                case 4 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:193:4: VAR
                    {
                    VAR34=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression867); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR34);



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
                    // 193:8: ->
                    {
                        adaptor.addChild(root_0,  makePath(VAR34) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:194:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_arrayCreation_in_valueExpression876);
                    arrayCreation35=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation35.getTree());

                    }
                    break;
                case 6 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:195:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_objectCreation_in_valueExpression882);
                    objectCreation36=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation36.getTree());

                    }
                    break;
                case 7 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:196:4: operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_operatorExpression_in_valueExpression888);
                    operatorExpression37=operatorExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operatorExpression37.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 16, valueExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "valueExpression"

    public static class operatorExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:1: operatorExpression : operator ;
    public final SJaqlParser.operatorExpression_return operatorExpression() throws RecognitionException {
        SJaqlParser.operatorExpression_return retval = new SJaqlParser.operatorExpression_return();
        retval.start = input.LT(1);
        int operatorExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.operator_return operator38 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 17) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:2: ( operator )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:4: operator
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_operator_in_operatorExpression898);
            operator38=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, operator38.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 17, operatorExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "operatorExpression"

    public static class parenthesesExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "parenthesesExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:201:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
    public final SJaqlParser.parenthesesExpression_return parenthesesExpression() throws RecognitionException {
        SJaqlParser.parenthesesExpression_return retval = new SJaqlParser.parenthesesExpression_return();
        retval.start = input.LT(1);
        int parenthesesExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal39=null;
        Token char_literal41=null;
        SJaqlParser.expression_return expression40 = null;


        EvaluationExpression char_literal39_tree=null;
        EvaluationExpression char_literal41_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 18) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:2: ( ( '(' expression ')' ) -> expression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:4: ( '(' expression ')' )
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:4: ( '(' expression ')' )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:5: '(' expression ')'
            {
            char_literal39=(Token)match(input,47,FOLLOW_47_in_parenthesesExpression910); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_47.add(char_literal39);

            pushFollow(FOLLOW_expression_in_parenthesesExpression912);
            expression40=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression40.getTree());
            char_literal41=(Token)match(input,48,FOLLOW_48_in_parenthesesExpression914); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_48.add(char_literal41);


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
            // 202:25: -> expression
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
            if ( state.backtracking>0 ) { memoize(input, 18, parenthesesExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "parenthesesExpression"

    public static class functionCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionCall"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:204:1: functionCall : name= ID '(' (param= expression ( ',' param= expression )* )? ')' -> ^( EXPRESSION[\"FunctionCall\"] ) ;
    public final SJaqlParser.functionCall_return functionCall() throws RecognitionException {
        SJaqlParser.functionCall_return retval = new SJaqlParser.functionCall_return();
        retval.start = input.LT(1);
        int functionCall_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal42=null;
        Token char_literal43=null;
        Token char_literal44=null;
        SJaqlParser.expression_return param = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal42_tree=null;
        EvaluationExpression char_literal43_tree=null;
        EvaluationExpression char_literal44_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<EvaluationExpression> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 19) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:2: (name= ID '(' (param= expression ( ',' param= expression )* )? ')' -> ^( EXPRESSION[\"FunctionCall\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:4: name= ID '(' (param= expression ( ',' param= expression )* )? ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_functionCall935); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal42=(Token)match(input,47,FOLLOW_47_in_functionCall937); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_47.add(char_literal42);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:207:2: (param= expression ( ',' param= expression )* )?
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( ((LA22_0>=VAR && LA22_0<=ID)||(LA22_0>=DECIMAL && LA22_0<=UINT)||(LA22_0>=42 && LA22_0<=45)||LA22_0==47||LA22_0==51||(LA22_0>=53 && LA22_0<=55)||(LA22_0>=57 && LA22_0<=58)) ) {
                alt22=1;
            }
            switch (alt22) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:207:3: param= expression ( ',' param= expression )*
                    {
                    pushFollow(FOLLOW_expression_in_functionCall944);
                    param=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    if ( state.backtracking==0 ) {
                       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    }
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:208:2: ( ',' param= expression )*
                    loop21:
                    do {
                        int alt21=2;
                        int LA21_0 = input.LA(1);

                        if ( (LA21_0==49) ) {
                            alt21=1;
                        }


                        switch (alt21) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:208:3: ',' param= expression
                    	    {
                    	    char_literal43=(Token)match(input,49,FOLLOW_49_in_functionCall950); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_49.add(char_literal43);

                    	    pushFollow(FOLLOW_expression_in_functionCall954);
                    	    param=expression();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    	    if ( state.backtracking==0 ) {
                    	       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop21;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal44=(Token)match(input,48,FOLLOW_48_in_functionCall964); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_48.add(char_literal44);



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
            // 209:6: -> ^( EXPRESSION[\"FunctionCall\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:209:9: ^( EXPRESSION[\"FunctionCall\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "FunctionCall"), root_1);

                adaptor.addChild(root_1,  (name!=null?name.getText():null) );
                adaptor.addChild(root_1,  params.toArray(new EvaluationExpression[params.size()]) );

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
            if ( state.backtracking>0 ) { memoize(input, 19, functionCall_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:211:1: fieldAssignment returns [ObjectCreation.Mapping mapping] : ( VAR '.' STAR -> | VAR '.' ID -> | ID ':' expression ->);
    public final SJaqlParser.fieldAssignment_return fieldAssignment() throws RecognitionException {
        SJaqlParser.fieldAssignment_return retval = new SJaqlParser.fieldAssignment_return();
        retval.start = input.LT(1);
        int fieldAssignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR45=null;
        Token char_literal46=null;
        Token STAR47=null;
        Token VAR48=null;
        Token char_literal49=null;
        Token ID50=null;
        Token ID51=null;
        Token char_literal52=null;
        SJaqlParser.expression_return expression53 = null;


        EvaluationExpression VAR45_tree=null;
        EvaluationExpression char_literal46_tree=null;
        EvaluationExpression STAR47_tree=null;
        EvaluationExpression VAR48_tree=null;
        EvaluationExpression char_literal49_tree=null;
        EvaluationExpression ID50_tree=null;
        EvaluationExpression ID51_tree=null;
        EvaluationExpression char_literal52_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 20) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:212:2: ( VAR '.' STAR -> | VAR '.' ID -> | ID ':' expression ->)
            int alt23=3;
            int LA23_0 = input.LA(1);

            if ( (LA23_0==VAR) ) {
                int LA23_1 = input.LA(2);

                if ( (LA23_1==46) ) {
                    int LA23_3 = input.LA(3);

                    if ( (LA23_3==STAR) ) {
                        alt23=1;
                    }
                    else if ( (LA23_3==ID) ) {
                        alt23=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 23, 3, input);

                        throw nvae;
                    }
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 23, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA23_0==ID) ) {
                alt23=3;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 23, 0, input);

                throw nvae;
            }
            switch (alt23) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:212:4: VAR '.' STAR
                    {
                    VAR45=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment989); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR45);

                    char_literal46=(Token)match(input,46,FOLLOW_46_in_fieldAssignment991); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_46.add(char_literal46);

                    STAR47=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment993); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR47);

                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.CopyFields(makePath(VAR45))); 
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
                    // 212:99: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:4: VAR '.' ID
                    {
                    VAR48=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1002); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR48);

                    char_literal49=(Token)match(input,46,FOLLOW_46_in_fieldAssignment1004); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_46.add(char_literal49);

                    ID50=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1006); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID50);

                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.Mapping((ID50!=null?ID50.getText():null), makePath(VAR48, (ID50!=null?ID50.getText():null)))); 
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
                    // 213:114: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:214:4: ID ':' expression
                    {
                    ID51=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1015); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID51);

                    char_literal52=(Token)match(input,50,FOLLOW_50_in_fieldAssignment1017); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_50.add(char_literal52);

                    pushFollow(FOLLOW_expression_in_fieldAssignment1019);
                    expression53=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression53.getTree());
                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.Mapping((ID51!=null?ID51.getText():null), (expression53!=null?((EvaluationExpression)expression53.tree):null))); 
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
                    // 214:113: ->
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
            if ( state.backtracking>0 ) { memoize(input, 20, fieldAssignment_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:216:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
    public final SJaqlParser.objectCreation_return objectCreation() throws RecognitionException {
        objectCreation_stack.push(new objectCreation_scope());
        SJaqlParser.objectCreation_return retval = new SJaqlParser.objectCreation_return();
        retval.start = input.LT(1);
        int objectCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal54=null;
        Token char_literal56=null;
        Token char_literal58=null;
        Token char_literal59=null;
        SJaqlParser.fieldAssignment_return fieldAssignment55 = null;

        SJaqlParser.fieldAssignment_return fieldAssignment57 = null;


        EvaluationExpression char_literal54_tree=null;
        EvaluationExpression char_literal56_tree=null;
        EvaluationExpression char_literal58_tree=null;
        EvaluationExpression char_literal59_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 21) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal54=(Token)match(input,51,FOLLOW_51_in_objectCreation1041); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_51.add(char_literal54);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( ((LA26_0>=VAR && LA26_0<=ID)) ) {
                alt26=1;
            }
            switch (alt26) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1044);
                    fieldAssignment55=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment55.getTree());
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:25: ( ',' fieldAssignment )*
                    loop24:
                    do {
                        int alt24=2;
                        int LA24_0 = input.LA(1);

                        if ( (LA24_0==49) ) {
                            int LA24_1 = input.LA(2);

                            if ( ((LA24_1>=VAR && LA24_1<=ID)) ) {
                                alt24=1;
                            }


                        }


                        switch (alt24) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:26: ',' fieldAssignment
                    	    {
                    	    char_literal56=(Token)match(input,49,FOLLOW_49_in_objectCreation1047); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_49.add(char_literal56);

                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1049);
                    	    fieldAssignment57=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment57.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop24;
                        }
                    } while (true);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:48: ( ',' )?
                    int alt25=2;
                    int LA25_0 = input.LA(1);

                    if ( (LA25_0==49) ) {
                        alt25=1;
                    }
                    switch (alt25) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                            {
                            char_literal58=(Token)match(input,49,FOLLOW_49_in_objectCreation1053); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(char_literal58);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal59=(Token)match(input,52,FOLLOW_52_in_objectCreation1058); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_52.add(char_literal59);



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
            // 219:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:62: ^( EXPRESSION[\"ObjectCreation\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 21, objectCreation_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:225:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) );
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 22) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:226:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) )
            int alt27=6;
            switch ( input.LA(1) ) {
            case 53:
                {
                alt27=1;
                }
                break;
            case 54:
                {
                alt27=2;
                }
                break;
            case DECIMAL:
                {
                alt27=3;
                }
                break;
            case STRING:
                {
                alt27=4;
                }
                break;
            case INTEGER:
                {
                alt27=5;
                }
                break;
            case UINT:
                {
                alt27=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 27, 0, input);

                throw nvae;
            }

            switch (alt27) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:226:4: val= 'true'
                    {
                    val=(Token)match(input,53,FOLLOW_53_in_literal1082); if (state.failed) return retval; 
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
                    // 226:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:226:18: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:227:4: val= 'false'
                    {
                    val=(Token)match(input,54,FOLLOW_54_in_literal1098); if (state.failed) return retval; 
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
                    // 227:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:227:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal1114); if (state.failed) return retval; 
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
                    // 228:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:228:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal1130); if (state.failed) return retval; 
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
                    // 229:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:18: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:5: val= INTEGER
                    {
                    val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal1147); if (state.failed) return retval; 
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
                    // 230:17: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:20: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:231:5: val= UINT
                    {
                    val=(Token)match(input,UINT,FOLLOW_UINT_in_literal1164); if (state.failed) return retval; 
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
                    // 231:14: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:231:17: ^( EXPRESSION[\"ConstantExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 22, literal_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "literal"

    public static class arrayAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:1: arrayAccess : ( '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
    public final SJaqlParser.arrayAccess_return arrayAccess() throws RecognitionException {
        SJaqlParser.arrayAccess_return retval = new SJaqlParser.arrayAccess_return();
        retval.start = input.LT(1);
        int arrayAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token pos=null;
        Token start=null;
        Token end=null;
        Token char_literal60=null;
        Token STAR61=null;
        Token char_literal62=null;
        Token char_literal63=null;
        Token char_literal64=null;
        Token char_literal65=null;
        Token char_literal66=null;
        Token char_literal67=null;

        EvaluationExpression pos_tree=null;
        EvaluationExpression start_tree=null;
        EvaluationExpression end_tree=null;
        EvaluationExpression char_literal60_tree=null;
        EvaluationExpression STAR61_tree=null;
        EvaluationExpression char_literal62_tree=null;
        EvaluationExpression char_literal63_tree=null;
        EvaluationExpression char_literal64_tree=null;
        EvaluationExpression char_literal65_tree=null;
        EvaluationExpression char_literal66_tree=null;
        EvaluationExpression char_literal67_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 23) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:234:3: ( '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt31=3;
            int LA31_0 = input.LA(1);

            if ( (LA31_0==55) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt31=1;
                    }
                    break;
                case INTEGER:
                    {
                    int LA31_3 = input.LA(3);

                    if ( (LA31_3==56) ) {
                        alt31=2;
                    }
                    else if ( (LA31_3==50) ) {
                        alt31=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 31, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case UINT:
                    {
                    int LA31_4 = input.LA(3);

                    if ( (LA31_4==56) ) {
                        alt31=2;
                    }
                    else if ( (LA31_4==50) ) {
                        alt31=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 31, 4, input);

                        throw nvae;
                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 31, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;
            }
            switch (alt31) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:234:5: '[' STAR ']'
                    {
                    char_literal60=(Token)match(input,55,FOLLOW_55_in_arrayAccess1183); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_55.add(char_literal60);

                    STAR61=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1185); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR61);

                    char_literal62=(Token)match(input,56,FOLLOW_56_in_arrayAccess1187); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal62);



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
                    // 235:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess"), root_1);

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:236:5: '[' (pos= INTEGER | pos= UINT ) ']'
                    {
                    char_literal63=(Token)match(input,55,FOLLOW_55_in_arrayAccess1205); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_55.add(char_literal63);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:236:9: (pos= INTEGER | pos= UINT )
                    int alt28=2;
                    int LA28_0 = input.LA(1);

                    if ( (LA28_0==INTEGER) ) {
                        alt28=1;
                    }
                    else if ( (LA28_0==UINT) ) {
                        alt28=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 28, 0, input);

                        throw nvae;
                    }
                    switch (alt28) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:236:10: pos= INTEGER
                            {
                            pos=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1210); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(pos);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:236:24: pos= UINT
                            {
                            pos=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1216); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(pos);


                            }
                            break;

                    }

                    char_literal64=(Token)match(input,56,FOLLOW_56_in_arrayAccess1219); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal64);



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
                    // 237:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:6: ^( EXPRESSION[\"ArrayAccess\"] )
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
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:5: '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']'
                    {
                    char_literal65=(Token)match(input,55,FOLLOW_55_in_arrayAccess1237); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_55.add(char_literal65);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:9: (start= INTEGER | start= UINT )
                    int alt29=2;
                    int LA29_0 = input.LA(1);

                    if ( (LA29_0==INTEGER) ) {
                        alt29=1;
                    }
                    else if ( (LA29_0==UINT) ) {
                        alt29=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 29, 0, input);

                        throw nvae;
                    }
                    switch (alt29) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:10: start= INTEGER
                            {
                            start=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1242); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(start);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:26: start= UINT
                            {
                            start=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1248); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(start);


                            }
                            break;

                    }

                    char_literal66=(Token)match(input,50,FOLLOW_50_in_arrayAccess1251); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_50.add(char_literal66);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:42: (end= INTEGER | end= UINT )
                    int alt30=2;
                    int LA30_0 = input.LA(1);

                    if ( (LA30_0==INTEGER) ) {
                        alt30=1;
                    }
                    else if ( (LA30_0==UINT) ) {
                        alt30=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 30, 0, input);

                        throw nvae;
                    }
                    switch (alt30) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:43: end= INTEGER
                            {
                            end=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1256); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(end);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:57: end= UINT
                            {
                            end=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1262); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(end);


                            }
                            break;

                    }

                    char_literal67=(Token)match(input,56,FOLLOW_56_in_arrayAccess1265); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal67);



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
                    // 239:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:239:6: ^( EXPRESSION[\"ArrayAccess\"] )
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

            }
            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 23, arrayAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayCreation"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:241:1: arrayCreation : '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] $elems) ;
    public final SJaqlParser.arrayCreation_return arrayCreation() throws RecognitionException {
        SJaqlParser.arrayCreation_return retval = new SJaqlParser.arrayCreation_return();
        retval.start = input.LT(1);
        int arrayCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal68=null;
        Token char_literal69=null;
        Token char_literal70=null;
        Token char_literal71=null;
        List list_elems=null;
        RuleReturnScope elems = null;
        EvaluationExpression char_literal68_tree=null;
        EvaluationExpression char_literal69_tree=null;
        EvaluationExpression char_literal70_tree=null;
        EvaluationExpression char_literal71_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 24) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:2: ( '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] $elems) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:5: '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']'
            {
            char_literal68=(Token)match(input,55,FOLLOW_55_in_arrayCreation1290); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_55.add(char_literal68);

            pushFollow(FOLLOW_expression_in_arrayCreation1294);
            elems=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            if (list_elems==null) list_elems=new ArrayList();
            list_elems.add(elems.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:27: ( ',' elems+= expression )*
            loop32:
            do {
                int alt32=2;
                int LA32_0 = input.LA(1);

                if ( (LA32_0==49) ) {
                    int LA32_1 = input.LA(2);

                    if ( ((LA32_1>=VAR && LA32_1<=ID)||(LA32_1>=DECIMAL && LA32_1<=UINT)||(LA32_1>=42 && LA32_1<=45)||LA32_1==47||LA32_1==51||(LA32_1>=53 && LA32_1<=55)||(LA32_1>=57 && LA32_1<=58)) ) {
                        alt32=1;
                    }


                }


                switch (alt32) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:28: ',' elems+= expression
            	    {
            	    char_literal69=(Token)match(input,49,FOLLOW_49_in_arrayCreation1297); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_49.add(char_literal69);

            	    pushFollow(FOLLOW_expression_in_arrayCreation1301);
            	    elems=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            	    if (list_elems==null) list_elems=new ArrayList();
            	    list_elems.add(elems.getTree());


            	    }
            	    break;

            	default :
            	    break loop32;
                }
            } while (true);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:52: ( ',' )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==49) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                    {
                    char_literal70=(Token)match(input,49,FOLLOW_49_in_arrayCreation1305); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal70);


                    }
                    break;

            }

            char_literal71=(Token)match(input,56,FOLLOW_56_in_arrayCreation1308); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_56.add(char_literal71);



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
            // 242:61: -> ^( EXPRESSION[\"ArrayCreation\"] $elems)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:64: ^( EXPRESSION[\"ArrayCreation\"] $elems)
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
            if ( state.backtracking>0 ) { memoize(input, 24, arrayCreation_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayCreation"

    protected static class operator_scope {
        List<String> inputNames;
        java.util.BitSet hasExplicitName;
        List<List<ExpressionTag>> inputTags;
        Operator result;
    }
    protected Stack operator_stack = new Stack();

    public static class operator_return extends ParserRuleReturnScope {
        public Operator op=null;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:244:1: operator returns [Operator op=null] : opRule= ( readOperator | writeOperator | genericOperator ) ;
    public final SJaqlParser.operator_return operator() throws RecognitionException {
        operator_stack.push(new operator_scope());
        SJaqlParser.operator_return retval = new SJaqlParser.operator_return();
        retval.start = input.LT(1);
        int operator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token opRule=null;
        SJaqlParser.readOperator_return readOperator72 = null;

        SJaqlParser.writeOperator_return writeOperator73 = null;

        SJaqlParser.genericOperator_return genericOperator74 = null;


        EvaluationExpression opRule_tree=null;

         
          ((operator_scope)operator_stack.peek()).inputNames = new ArrayList<String>();
          ((operator_scope)operator_stack.peek()).inputTags = new ArrayList<List<ExpressionTag>>();
          ((operator_scope)operator_stack.peek()).hasExplicitName = new java.util.BitSet();

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 25) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:2: (opRule= ( readOperator | writeOperator | genericOperator ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:4: opRule= ( readOperator | writeOperator | genericOperator )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:11: ( readOperator | writeOperator | genericOperator )
            int alt34=3;
            switch ( input.LA(1) ) {
            case 57:
                {
                alt34=1;
                }
                break;
            case 58:
                {
                alt34=2;
                }
                break;
            case ID:
                {
                alt34=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 34, 0, input);

                throw nvae;
            }

            switch (alt34) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:12: readOperator
                    {
                    pushFollow(FOLLOW_readOperator_in_operator1343);
                    readOperator72=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator72.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:27: writeOperator
                    {
                    pushFollow(FOLLOW_writeOperator_in_operator1347);
                    writeOperator73=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator73.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:43: genericOperator
                    {
                    pushFollow(FOLLOW_genericOperator_in_operator1351);
                    genericOperator74=genericOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, genericOperator74.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 25, operator_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:261:1: readOperator : 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.readOperator_return readOperator() throws RecognitionException {
        SJaqlParser.readOperator_return retval = new SJaqlParser.readOperator_return();
        retval.start = input.LT(1);
        int readOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token loc=null;
        Token file=null;
        Token string_literal75=null;
        Token char_literal76=null;
        Token char_literal77=null;

        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal75_tree=null;
        EvaluationExpression char_literal76_tree=null;
        EvaluationExpression char_literal77_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 26) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:262:2: ( 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:262:4: 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal75=(Token)match(input,57,FOLLOW_57_in_readOperator1365); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_57.add(string_literal75);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:262:11: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt36=2;
            int LA36_0 = input.LA(1);

            if ( (LA36_0==ID) ) {
                int LA36_1 = input.LA(2);

                if ( (LA36_1==47) ) {
                    alt36=2;
                }
                else if ( (LA36_1==STRING) ) {
                    alt36=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 36, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA36_0==STRING) ) {
                alt36=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 36, 0, input);

                throw nvae;
            }
            switch (alt36) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:262:12: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:262:15: (loc= ID )?
                    int alt35=2;
                    int LA35_0 = input.LA(1);

                    if ( (LA35_0==ID) ) {
                        alt35=1;
                    }
                    switch (alt35) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1370); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1375); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:262:34: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1381); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal76=(Token)match(input,47,FOLLOW_47_in_readOperator1383); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_47.add(char_literal76);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1387); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal77=(Token)match(input,48,FOLLOW_48_in_readOperator1389); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal77);


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
            // 262:133: ->
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
            if ( state.backtracking>0 ) { memoize(input, 26, readOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "readOperator"

    public static class writeOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "writeOperator"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:264:1: writeOperator : 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.writeOperator_return writeOperator() throws RecognitionException {
        SJaqlParser.writeOperator_return retval = new SJaqlParser.writeOperator_return();
        retval.start = input.LT(1);
        int writeOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token loc=null;
        Token file=null;
        Token string_literal78=null;
        Token string_literal79=null;
        Token char_literal80=null;
        Token char_literal81=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal78_tree=null;
        EvaluationExpression string_literal79_tree=null;
        EvaluationExpression char_literal80_tree=null;
        EvaluationExpression char_literal81_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 27) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:2: ( 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:4: 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal78=(Token)match(input,58,FOLLOW_58_in_writeOperator1403); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_58.add(string_literal78);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator1407); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            string_literal79=(Token)match(input,59,FOLLOW_59_in_writeOperator1409); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(string_literal79);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:26: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt38=2;
            int LA38_0 = input.LA(1);

            if ( (LA38_0==ID) ) {
                int LA38_1 = input.LA(2);

                if ( (LA38_1==47) ) {
                    alt38=2;
                }
                else if ( (LA38_1==STRING) ) {
                    alt38=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 38, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA38_0==STRING) ) {
                alt38=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 38, 0, input);

                throw nvae;
            }
            switch (alt38) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:27: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:30: (loc= ID )?
                    int alt37=2;
                    int LA37_0 = input.LA(1);

                    if ( (LA37_0==ID) ) {
                        alt37=1;
                    }
                    switch (alt37) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1414); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1419); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:49: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1425); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal80=(Token)match(input,47,FOLLOW_47_in_writeOperator1427); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_47.add(char_literal80);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1431); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal81=(Token)match(input,48,FOLLOW_48_in_writeOperator1433); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal81);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
              	Sink sink = new Sink(JsonOutputFormat.class, (file!=null?file.getText():null));
                ((operator_scope)operator_stack.peek()).result = sink;
                sink.setInputs(getVariable(from));
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
            // 271:3: ->
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
            if ( state.backtracking>0 ) { memoize(input, 27, writeOperator_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:273:1: genericOperator : name= ID ({...}?moreName= ID )? ( operatorFlag )* input ( ',' input )* ( operatorOption )* ->;
    public final SJaqlParser.genericOperator_return genericOperator() throws RecognitionException {
        genericOperator_stack.push(new genericOperator_scope());
        SJaqlParser.genericOperator_return retval = new SJaqlParser.genericOperator_return();
        retval.start = input.LT(1);
        int genericOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token moreName=null;
        Token char_literal84=null;
        SJaqlParser.operatorFlag_return operatorFlag82 = null;

        SJaqlParser.input_return input83 = null;

        SJaqlParser.input_return input85 = null;

        SJaqlParser.operatorOption_return operatorOption86 = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression moreName_tree=null;
        EvaluationExpression char_literal84_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_operatorOption=new RewriteRuleSubtreeStream(adaptor,"rule operatorOption");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        RewriteRuleSubtreeStream stream_operatorFlag=new RewriteRuleSubtreeStream(adaptor,"rule operatorFlag");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 28) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:277:2: (name= ID ({...}?moreName= ID )? ( operatorFlag )* input ( ',' input )* ( operatorOption )* ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:277:4: name= ID ({...}?moreName= ID )? ( operatorFlag )* input ( ',' input )* ( operatorOption )*
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1454); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
              ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = operatorFactory.getOperatorInfo((name!=null?name.getText():null));
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:278:1: ({...}?moreName= ID )?
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==ID) ) {
                int LA39_1 = input.LA(2);

                if ( ((synpred60_SJaql()&&(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo == null))) ) {
                    alt39=1;
                }
            }
            switch (alt39) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:278:2: {...}?moreName= ID
                    {
                    if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo == null)) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "genericOperator", "$genericOperator::operatorInfo == null");
                    }
                    moreName=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1463); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(moreName);

                    if ( state.backtracking==0 ) {
                      ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = operatorFactory.getOperatorInfo((name!=null?name.getText():null) + " " + (moreName!=null?moreName.getText():null));
                    }

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
                if(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo == null)
                  throw new IllegalArgumentException("Unknown operator:", new RecognitionException(name.getInputStream()));
                ((operator_scope)operator_stack.peek()).result = ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.newInstance();

            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:285:1: ( operatorFlag )*
            loop40:
            do {
                int alt40=2;
                int LA40_0 = input.LA(1);

                if ( (LA40_0==ID) ) {
                    alt40=1;
                }


                switch (alt40) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: operatorFlag
            	    {
            	    pushFollow(FOLLOW_operatorFlag_in_genericOperator1474);
            	    operatorFlag82=operatorFlag();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorFlag.add(operatorFlag82.getTree());

            	    }
            	    break;

            	default :
            	    break loop40;
                }
            } while (true);

            pushFollow(FOLLOW_input_in_genericOperator1477);
            input83=input();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_input.add(input83.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:286:7: ( ',' input )*
            loop41:
            do {
                int alt41=2;
                int LA41_0 = input.LA(1);

                if ( (LA41_0==49) ) {
                    int LA41_2 = input.LA(2);

                    if ( (synpred62_SJaql()) ) {
                        alt41=1;
                    }


                }


                switch (alt41) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:286:8: ',' input
            	    {
            	    char_literal84=(Token)match(input,49,FOLLOW_49_in_genericOperator1480); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_49.add(char_literal84);

            	    pushFollow(FOLLOW_input_in_genericOperator1482);
            	    input85=input();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_input.add(input85.getTree());

            	    }
            	    break;

            	default :
            	    break loop41;
                }
            } while (true);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:1: ( operatorOption )*
            loop42:
            do {
                int alt42=2;
                int LA42_0 = input.LA(1);

                if ( (LA42_0==ID) ) {
                    int LA42_2 = input.LA(2);

                    if ( (synpred63_SJaql()) ) {
                        alt42=1;
                    }


                }


                switch (alt42) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: operatorOption
            	    {
            	    pushFollow(FOLLOW_operatorOption_in_genericOperator1487);
            	    operatorOption86=operatorOption();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorOption.add(operatorOption86.getTree());

            	    }
            	    break;

            	default :
            	    break loop42;
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
            // 287:17: ->
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
            if ( state.backtracking>0 ) { memoize(input, 28, genericOperator_StartIndex); }
            genericOperator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "genericOperator"

    protected static class operatorOption_scope {
        String optionName;
    }
    protected Stack operatorOption_stack = new Stack();

    public static class operatorOption_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorOption"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:289:1: operatorOption : name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null] ->;
    public final SJaqlParser.operatorOption_return operatorOption() throws RecognitionException {
        operatorOption_stack.push(new operatorOption_scope());
        SJaqlParser.operatorOption_return retval = new SJaqlParser.operatorOption_return();
        retval.start = input.LT(1);
        int operatorOption_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token moreName=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression moreName_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 29) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:293:2: (name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null] ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:293:4: name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1507); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
               ((operatorOption_scope)operatorOption_stack.peek()).optionName = (name!=null?name.getText():null); 
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:294:1: ({...}?moreName= ID )?
            int alt43=2;
            alt43 = dfa43.predict(input);
            switch (alt43) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:294:2: {...}?moreName= ID
                    {
                    if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "operatorOption", "!$genericOperator::operatorInfo.hasProperty($operatorOption::optionName)");
                    }
                    moreName=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1516); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(moreName);

                    if ( state.backtracking==0 ) {
                       ((operatorOption_scope)operatorOption_stack.peek()).optionName = (name!=null?name.getText():null) + " " + (moreName!=null?moreName.getText():null);
                    }

                    }
                    break;

            }

            pushFollow(FOLLOW_contextAwareExpression_in_operatorOption1526);
            expr=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
            if ( state.backtracking==0 ) {
               ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.setProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName, ((operator_scope)operator_stack.peek()).result, (expr!=null?((EvaluationExpression)expr.tree):null)); 
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
            // 296:143: ->
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
            if ( state.backtracking>0 ) { memoize(input, 29, operatorOption_StartIndex); }
            operatorOption_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorOption"

    protected static class operatorFlag_scope {
        String flagName;
    }
    protected Stack operatorFlag_stack = new Stack();

    public static class operatorFlag_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorFlag"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:298:1: operatorFlag : name= ID ({...}?moreName= ID )? ->;
    public final SJaqlParser.operatorFlag_return operatorFlag() throws RecognitionException {
        operatorFlag_stack.push(new operatorFlag_scope());
        SJaqlParser.operatorFlag_return retval = new SJaqlParser.operatorFlag_return();
        retval.start = input.LT(1);
        int operatorFlag_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token moreName=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression moreName_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 30) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:302:3: (name= ID ({...}?moreName= ID )? ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:302:5: name= ID ({...}?moreName= ID )?
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1547); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
               ((operatorFlag_scope)operatorFlag_stack.peek()).flagName = (name!=null?name.getText():null); 
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:303:1: ({...}?moreName= ID )?
            int alt44=2;
            int LA44_0 = input.LA(1);

            if ( (LA44_0==ID) ) {
                int LA44_1 = input.LA(2);

                if ( ((synpred65_SJaql()&&(!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName)))) ) {
                    alt44=1;
                }
            }
            switch (alt44) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:303:2: {...}?moreName= ID
                    {
                    if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "operatorFlag", "!$genericOperator::operatorInfo.hasFlag($operatorFlag::flagName)");
                    }
                    moreName=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1557); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(moreName);

                    if ( state.backtracking==0 ) {
                       ((operatorFlag_scope)operatorFlag_stack.peek()).flagName = (name!=null?name.getText():null) + " " + (moreName!=null?moreName.getText():null);
                    }

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.setProperty(((operatorFlag_scope)operatorFlag_stack.peek()).flagName, ((operator_scope)operator_stack.peek()).result, true); 
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
            // 305:99: ->
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
            if ( state.backtracking>0 ) { memoize(input, 30, operatorFlag_StartIndex); }
            operatorFlag_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorFlag"

    public static class input_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "input"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:307:1: input : (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )? ->;
    public final SJaqlParser.input_return input() throws RecognitionException {
        SJaqlParser.input_return retval = new SJaqlParser.input_return();
        retval.start = input.LT(1);
        int input_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token preserveFlag=null;
        Token name=null;
        Token from=null;
        Token inputOption=null;
        Token string_literal87=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        EvaluationExpression preserveFlag_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression inputOption_tree=null;
        EvaluationExpression string_literal87_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 31) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:2: ( (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )? ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:4: (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:16: (preserveFlag= 'preserve' )?
            int alt45=2;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==60) ) {
                alt45=1;
            }
            switch (alt45) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: preserveFlag= 'preserve'
                    {
                    preserveFlag=(Token)match(input,60,FOLLOW_60_in_input1579); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_60.add(preserveFlag);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:32: (name= VAR 'in' )?
            int alt46=2;
            int LA46_0 = input.LA(1);

            if ( (LA46_0==VAR) ) {
                int LA46_1 = input.LA(2);

                if ( (LA46_1==31) ) {
                    int LA46_2 = input.LA(3);

                    if ( (LA46_2==VAR) ) {
                        int LA46_4 = input.LA(4);

                        if ( (synpred67_SJaql()) ) {
                            alt46=1;
                        }
                    }
                }
            }
            switch (alt46) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:33: name= VAR 'in'
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input1587); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);

                    string_literal87=(Token)match(input,31,FOLLOW_31_in_input1589); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(string_literal87);


                    }
                    break;

            }

            from=(Token)match(input,VAR,FOLLOW_VAR_in_input1595); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                int inputIndex = ((operator_scope)operator_stack.peek()).inputNames.size();
                ((operator_scope)operator_stack.peek()).result.setInput(inputIndex, getVariable(from));
                ((operator_scope)operator_stack.peek()).inputNames.add(name != null ? name.getText() : from.getText());
                ((operator_scope)operator_stack.peek()).hasExplicitName.set(inputIndex, name != null); 
                ((operator_scope)operator_stack.peek()).inputTags.add(preserveFlag == null ? new ArrayList<ExpressionTag>() : Arrays.asList(ExpressionTag.RETAIN));

            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:316:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?
            int alt47=2;
            alt47 = dfa47.predict(input);
            switch (alt47) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:316:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)]
                    {
                    inputOption=(Token)match(input,ID,FOLLOW_ID_in_input1603); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(inputOption);

                    if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasInputProperty((inputOption!=null?inputOption.getText():null)))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "input", "$genericOperator::operatorInfo.hasInputProperty($inputOption.text)");
                    }
                    pushFollow(FOLLOW_contextAwareExpression_in_input1612);
                    expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).inputNames.size() - 1));

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
                    if ( state.backtracking==0 ) {
                       ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.setInputProperty((inputOption!=null?inputOption.getText():null), ((operator_scope)operator_stack.peek()).result, ((operator_scope)operator_stack.peek()).inputNames.size()-1, (expr!=null?((EvaluationExpression)expr.tree):null)); 
                    }

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
            // 318:1: ->
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
            if ( state.backtracking>0 ) { memoize(input, 31, input_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "input"

    // $ANTLR start synpred4_SJaql
    public final void synpred4_SJaql_fragment() throws RecognitionException {   
        List list_exprs=null;
        RuleReturnScope exprs = null;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:27: ( ( 'or' | '||' ) exprs+= andExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:27: ( 'or' | '||' ) exprs+= andExpression
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

        pushFollow(FOLLOW_andExpression_in_synpred4_SJaql242);
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
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:31: ( ( 'and' | '&&' ) exprs+= elementExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:31: ( 'and' | '&&' ) exprs+= elementExpression
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

        pushFollow(FOLLOW_elementExpression_in_synpred6_SJaql289);
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
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:136:4: ( comparisonExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:136:4: comparisonExpression
        {
        pushFollow(FOLLOW_comparisonExpression_in_synpred7_SJaql321);
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


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:137:4: (elem= comparisonExpression 'in' set= comparisonExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:137:4: elem= comparisonExpression 'in' set= comparisonExpression
        {
        pushFollow(FOLLOW_comparisonExpression_in_synpred8_SJaql328);
        elem=comparisonExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,31,FOLLOW_31_in_synpred8_SJaql330); if (state.failed) return ;
        pushFollow(FOLLOW_comparisonExpression_in_synpred8_SJaql334);
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


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:29: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
        int alt48=6;
        switch ( input.LA(1) ) {
        case 33:
            {
            alt48=1;
            }
            break;
        case 34:
            {
            alt48=2;
            }
            break;
        case 35:
            {
            alt48=3;
            }
            break;
        case 36:
            {
            alt48=4;
            }
            break;
        case 37:
            {
            alt48=5;
            }
            break;
        case 38:
            {
            alt48=6;
            }
            break;
        default:
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 48, 0, input);

            throw nvae;
        }

        switch (alt48) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:30: s= '<='
                {
                s=(Token)match(input,33,FOLLOW_33_in_synpred14_SJaql395); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:39: s= '>='
                {
                s=(Token)match(input,34,FOLLOW_34_in_synpred14_SJaql401); if (state.failed) return ;

                }
                break;
            case 3 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:48: s= '<'
                {
                s=(Token)match(input,35,FOLLOW_35_in_synpred14_SJaql407); if (state.failed) return ;

                }
                break;
            case 4 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:56: s= '>'
                {
                s=(Token)match(input,36,FOLLOW_36_in_synpred14_SJaql413); if (state.failed) return ;

                }
                break;
            case 5 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:64: s= '=='
                {
                s=(Token)match(input,37,FOLLOW_37_in_synpred14_SJaql419); if (state.failed) return ;

                }
                break;
            case 6 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:73: s= '!='
                {
                s=(Token)match(input,38,FOLLOW_38_in_synpred14_SJaql425); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_arithmeticExpression_in_synpred14_SJaql430);
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


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:33: ( (s= '+' | s= '-' ) e2= multiplicationExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:33: (s= '+' | s= '-' ) e2= multiplicationExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:33: (s= '+' | s= '-' )
        int alt49=2;
        int LA49_0 = input.LA(1);

        if ( (LA49_0==39) ) {
            alt49=1;
        }
        else if ( (LA49_0==40) ) {
            alt49=2;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 49, 0, input);

            throw nvae;
        }
        switch (alt49) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:34: s= '+'
                {
                s=(Token)match(input,39,FOLLOW_39_in_synpred16_SJaql516); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:42: s= '-'
                {
                s=(Token)match(input,40,FOLLOW_40_in_synpred16_SJaql522); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_multiplicationExpression_in_synpred16_SJaql527);
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


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:31: ( (s= '*' | s= '/' ) e2= preincrementExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:31: (s= '*' | s= '/' ) e2= preincrementExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:31: (s= '*' | s= '/' )
        int alt50=2;
        int LA50_0 = input.LA(1);

        if ( (LA50_0==STAR) ) {
            alt50=1;
        }
        else if ( (LA50_0==41) ) {
            alt50=2;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 50, 0, input);

            throw nvae;
        }
        switch (alt50) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:32: s= '*'
                {
                s=(Token)match(input,STAR,FOLLOW_STAR_in_synpred18_SJaql576); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:154:40: s= '/'
                {
                s=(Token)match(input,41,FOLLOW_41_in_synpred18_SJaql582); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_preincrementExpression_in_synpred18_SJaql587);
        e2=preincrementExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred18_SJaql

    // $ANTLR start synpred23_SJaql
    public final void synpred23_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:5: ( ({...}? contextAwarePathExpression ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:5: ({...}? contextAwarePathExpression )
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:5: ({...}? contextAwarePathExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:6: {...}? contextAwarePathExpression
        {
        if ( !((((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred23_SJaql", "$contextAwareExpression::context != null");
        }
        pushFollow(FOLLOW_contextAwarePathExpression_in_synpred23_SJaql669);
        contextAwarePathExpression();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred23_SJaql

    // $ANTLR start synpred24_SJaql
    public final void synpred24_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:7: ( ( '.' (field= ID ) ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:7: ( '.' (field= ID ) )
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:7: ( '.' (field= ID ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:8: '.' (field= ID )
        {
        match(input,46,FOLLOW_46_in_synpred24_SJaql711); if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:12: (field= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred24_SJaql716); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred24_SJaql

    // $ANTLR start synpred25_SJaql
    public final void synpred25_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:11: ( arrayAccess )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:176:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred25_SJaql734);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred25_SJaql

    // $ANTLR start synpred26_SJaql
    public final void synpred26_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( ( '.' (field= ID ) ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( '.' (field= ID ) )
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( '.' (field= ID ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:8: '.' (field= ID )
        {
        match(input,46,FOLLOW_46_in_synpred26_SJaql790); if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:12: (field= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred26_SJaql795); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred26_SJaql

    // $ANTLR start synpred27_SJaql
    public final void synpred27_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:11: ( arrayAccess )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred27_SJaql813);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred27_SJaql

    // $ANTLR start synpred28_SJaql
    public final void synpred28_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:5: valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+
        {
        pushFollow(FOLLOW_valueExpression_in_synpred28_SJaql776);
        valueExpression();

        state._fsp--;
        if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:5: ( ( '.' (field= ID ) ) | arrayAccess )+
        int cnt51=0;
        loop51:
        do {
            int alt51=3;
            int LA51_0 = input.LA(1);

            if ( (LA51_0==46) ) {
                alt51=1;
            }
            else if ( (LA51_0==55) ) {
                alt51=2;
            }


            switch (alt51) {
        	case 1 :
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( '.' (field= ID ) )
        	    {
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:7: ( '.' (field= ID ) )
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:8: '.' (field= ID )
        	    {
        	    match(input,46,FOLLOW_46_in_synpred28_SJaql790); if (state.failed) return ;
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:12: (field= ID )
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:13: field= ID
        	    {
        	    field=(Token)match(input,ID,FOLLOW_ID_in_synpred28_SJaql795); if (state.failed) return ;

        	    }


        	    }


        	    }
        	    break;
        	case 2 :
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:11: arrayAccess
        	    {
        	    pushFollow(FOLLOW_arrayAccess_in_synpred28_SJaql813);
        	    arrayAccess();

        	    state._fsp--;
        	    if (state.failed) return ;

        	    }
        	    break;

        	default :
        	    if ( cnt51 >= 1 ) break loop51;
        	    if (state.backtracking>0) {state.failed=true; return ;}
                    EarlyExitException eee =
                        new EarlyExitException(51, input);
                    throw eee;
            }
            cnt51++;
        } while (true);


        }
    }
    // $ANTLR end synpred28_SJaql

    // $ANTLR start synpred60_SJaql
    public final void synpred60_SJaql_fragment() throws RecognitionException {   
        Token moreName=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:278:2: ({...}?moreName= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:278:2: {...}?moreName= ID
        {
        if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo == null)) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred60_SJaql", "$genericOperator::operatorInfo == null");
        }
        moreName=(Token)match(input,ID,FOLLOW_ID_in_synpred60_SJaql1463); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred60_SJaql

    // $ANTLR start synpred62_SJaql
    public final void synpred62_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:286:8: ( ',' input )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:286:8: ',' input
        {
        match(input,49,FOLLOW_49_in_synpred62_SJaql1480); if (state.failed) return ;
        pushFollow(FOLLOW_input_in_synpred62_SJaql1482);
        input();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred62_SJaql

    // $ANTLR start synpred63_SJaql
    public final void synpred63_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:1: ( operatorOption )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:1: operatorOption
        {
        pushFollow(FOLLOW_operatorOption_in_synpred63_SJaql1487);
        operatorOption();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred63_SJaql

    // $ANTLR start synpred64_SJaql
    public final void synpred64_SJaql_fragment() throws RecognitionException {   
        Token moreName=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:294:2: ({...}?moreName= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:294:2: {...}?moreName= ID
        {
        if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred64_SJaql", "!$genericOperator::operatorInfo.hasProperty($operatorOption::optionName)");
        }
        moreName=(Token)match(input,ID,FOLLOW_ID_in_synpred64_SJaql1516); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred64_SJaql

    // $ANTLR start synpred65_SJaql
    public final void synpred65_SJaql_fragment() throws RecognitionException {   
        Token moreName=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:303:2: ({...}?moreName= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:303:2: {...}?moreName= ID
        {
        if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred65_SJaql", "!$genericOperator::operatorInfo.hasFlag($operatorFlag::flagName)");
        }
        moreName=(Token)match(input,ID,FOLLOW_ID_in_synpred65_SJaql1557); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred65_SJaql

    // $ANTLR start synpred67_SJaql
    public final void synpred67_SJaql_fragment() throws RecognitionException {   
        Token name=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:33: (name= VAR 'in' )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:33: name= VAR 'in'
        {
        name=(Token)match(input,VAR,FOLLOW_VAR_in_synpred67_SJaql1587); if (state.failed) return ;
        match(input,31,FOLLOW_31_in_synpred67_SJaql1589); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred67_SJaql

    // $ANTLR start synpred68_SJaql
    public final void synpred68_SJaql_fragment() throws RecognitionException {   
        Token inputOption=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:316:2: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:316:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)]
        {
        inputOption=(Token)match(input,ID,FOLLOW_ID_in_synpred68_SJaql1603); if (state.failed) return ;
        if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasInputProperty((inputOption!=null?inputOption.getText():null)))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred68_SJaql", "$genericOperator::operatorInfo.hasInputProperty($inputOption.text)");
        }
        pushFollow(FOLLOW_contextAwareExpression_in_synpred68_SJaql1612);
        expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).inputNames.size() - 1));

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred68_SJaql

    // Delegated rules

    public final boolean synpred62_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred62_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred26_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred26_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred63_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred63_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
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
    public final boolean synpred28_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred28_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred27_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred27_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred65_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred65_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred64_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred64_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred68_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred68_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred60_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred60_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred67_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred67_SJaql_fragment(); // can never throw exception
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
    protected DFA19 dfa19 = new DFA19(this);
    protected DFA43 dfa43 = new DFA43(this);
    protected DFA47 dfa47 = new DFA47(this);
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
            return "135:1: elementExpression : ( comparisonExpression | elem= comparisonExpression 'in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) | elem= comparisonExpression 'not in' set= comparisonExpression -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) );";
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
                        if ( ((synpred7_SJaql()||(synpred7_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)))) ) {s = 17;}

                        else if ( ((synpred8_SJaql()||(synpred8_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)))) ) {s = 18;}

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
        "\1\1\17\uffff";
    static final String DFA17_minS =
        "\1\7\1\uffff\1\7\1\10\1\0\1\70\2\62\1\uffff\2\0\1\13\1\uffff\2\70"+
        "\1\0";
    static final String DFA17_maxS =
        "\1\70\1\uffff\1\7\1\14\1\0\3\70\1\uffff\2\0\1\14\1\uffff\2\70\1"+
        "\0";
    static final String DFA17_acceptS =
        "\1\uffff\1\3\6\uffff\1\1\3\uffff\1\2\3\uffff";
    static final String DFA17_specialS =
        "\4\uffff\1\2\4\uffff\1\0\1\3\4\uffff\1\1}>";
    static final String[] DFA17_transitionS = {
            "\2\1\20\uffff\1\1\1\uffff\17\1\4\uffff\1\2\1\uffff\2\1\2\uffff"+
            "\1\1\2\uffff\1\3\1\1",
            "",
            "\1\4",
            "\1\5\2\uffff\1\6\1\7",
            "\1\uffff",
            "\1\11",
            "\1\13\5\uffff\1\12",
            "\1\13\5\uffff\1\12",
            "",
            "\1\uffff",
            "\1\uffff",
            "\1\15\1\16",
            "",
            "\1\17",
            "\1\17",
            "\1\uffff"
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
            return "()* loopback of 175:5: ( ( '.' (field= ID ) ) | arrayAccess )*";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA17_9 = input.LA(1);

                         
                        int index17_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index17_9);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA17_15 = input.LA(1);

                         
                        int index17_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index17_15);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA17_4 = input.LA(1);

                         
                        int index17_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred24_SJaql()) ) {s = 8;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index17_4);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA17_10 = input.LA(1);

                         
                        int index17_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index17_10);
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
    static final String DFA19_eotS =
        "\20\uffff";
    static final String DFA19_eofS =
        "\20\uffff";
    static final String DFA19_minS =
        "\1\6\15\0\2\uffff";
    static final String DFA19_maxS =
        "\1\72\15\0\2\uffff";
    static final String DFA19_acceptS =
        "\16\uffff\1\1\1\2";
    static final String DFA19_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\2\uffff}>";
    static final String[] DFA19_transitionS = {
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

    static final short[] DFA19_eot = DFA.unpackEncodedString(DFA19_eotS);
    static final short[] DFA19_eof = DFA.unpackEncodedString(DFA19_eofS);
    static final char[] DFA19_min = DFA.unpackEncodedStringToUnsignedChars(DFA19_minS);
    static final char[] DFA19_max = DFA.unpackEncodedStringToUnsignedChars(DFA19_maxS);
    static final short[] DFA19_accept = DFA.unpackEncodedString(DFA19_acceptS);
    static final short[] DFA19_special = DFA.unpackEncodedString(DFA19_specialS);
    static final short[][] DFA19_transition;

    static {
        int numStates = DFA19_transitionS.length;
        DFA19_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA19_transition[i] = DFA.unpackEncodedString(DFA19_transitionS[i]);
        }
    }

    class DFA19 extends DFA {

        public DFA19(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 19;
            this.eot = DFA19_eot;
            this.eof = DFA19_eof;
            this.min = DFA19_min;
            this.max = DFA19_max;
            this.accept = DFA19_accept;
            this.special = DFA19_special;
            this.transition = DFA19_transition;
        }
        public String getDescription() {
            return "178:1: pathExpression : ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA19_1 = input.LA(1);

                         
                        int index19_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA19_2 = input.LA(1);

                         
                        int index19_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA19_3 = input.LA(1);

                         
                        int index19_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA19_4 = input.LA(1);

                         
                        int index19_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA19_5 = input.LA(1);

                         
                        int index19_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA19_6 = input.LA(1);

                         
                        int index19_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA19_7 = input.LA(1);

                         
                        int index19_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA19_8 = input.LA(1);

                         
                        int index19_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA19_9 = input.LA(1);

                         
                        int index19_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA19_10 = input.LA(1);

                         
                        int index19_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA19_11 = input.LA(1);

                         
                        int index19_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA19_12 = input.LA(1);

                         
                        int index19_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA19_13 = input.LA(1);

                         
                        int index19_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred28_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index19_13);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 19, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA43_eotS =
        "\22\uffff";
    static final String DFA43_eofS =
        "\22\uffff";
    static final String DFA43_minS =
        "\1\6\1\0\20\uffff";
    static final String DFA43_maxS =
        "\1\72\1\0\20\uffff";
    static final String DFA43_acceptS =
        "\2\uffff\1\2\16\uffff\1\1";
    static final String DFA43_specialS =
        "\1\uffff\1\0\20\uffff}>";
    static final String[] DFA43_transitionS = {
            "\1\2\1\1\1\uffff\4\2\35\uffff\4\2\1\uffff\1\2\3\uffff\1\2\1"+
            "\uffff\3\2\1\uffff\2\2",
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
            "",
            "",
            "",
            ""
    };

    static final short[] DFA43_eot = DFA.unpackEncodedString(DFA43_eotS);
    static final short[] DFA43_eof = DFA.unpackEncodedString(DFA43_eofS);
    static final char[] DFA43_min = DFA.unpackEncodedStringToUnsignedChars(DFA43_minS);
    static final char[] DFA43_max = DFA.unpackEncodedStringToUnsignedChars(DFA43_maxS);
    static final short[] DFA43_accept = DFA.unpackEncodedString(DFA43_acceptS);
    static final short[] DFA43_special = DFA.unpackEncodedString(DFA43_specialS);
    static final short[][] DFA43_transition;

    static {
        int numStates = DFA43_transitionS.length;
        DFA43_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA43_transition[i] = DFA.unpackEncodedString(DFA43_transitionS[i]);
        }
    }

    class DFA43 extends DFA {

        public DFA43(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 43;
            this.eot = DFA43_eot;
            this.eof = DFA43_eof;
            this.min = DFA43_min;
            this.max = DFA43_max;
            this.accept = DFA43_accept;
            this.special = DFA43_special;
            this.transition = DFA43_transition;
        }
        public String getDescription() {
            return "294:1: ({...}?moreName= ID )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA43_1 = input.LA(1);

                         
                        int index43_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( ((synpred64_SJaql()&&(!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName)))) ) {s = 17;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index43_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 43, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA47_eotS =
        "\33\uffff";
    static final String DFA47_eofS =
        "\1\2\32\uffff";
    static final String DFA47_minS =
        "\1\7\1\0\31\uffff";
    static final String DFA47_maxS =
        "\1\70\1\0\31\uffff";
    static final String DFA47_acceptS =
        "\2\uffff\1\2\27\uffff\1\1";
    static final String DFA47_specialS =
        "\1\uffff\1\0\31\uffff}>";
    static final String[] DFA47_transitionS = {
            "\1\1\1\2\20\uffff\1\2\1\uffff\17\2\4\uffff\1\2\1\uffff\2\2\2"+
            "\uffff\1\2\2\uffff\2\2",
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

    static final short[] DFA47_eot = DFA.unpackEncodedString(DFA47_eotS);
    static final short[] DFA47_eof = DFA.unpackEncodedString(DFA47_eofS);
    static final char[] DFA47_min = DFA.unpackEncodedStringToUnsignedChars(DFA47_minS);
    static final char[] DFA47_max = DFA.unpackEncodedStringToUnsignedChars(DFA47_maxS);
    static final short[] DFA47_accept = DFA.unpackEncodedString(DFA47_acceptS);
    static final short[] DFA47_special = DFA.unpackEncodedString(DFA47_specialS);
    static final short[][] DFA47_transition;

    static {
        int numStates = DFA47_transitionS.length;
        DFA47_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA47_transition[i] = DFA.unpackEncodedString(DFA47_transitionS[i]);
        }
    }

    class DFA47 extends DFA {

        public DFA47(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 47;
            this.eot = DFA47_eot;
            this.eof = DFA47_eof;
            this.min = DFA47_min;
            this.max = DFA47_max;
            this.accept = DFA47_accept;
            this.special = DFA47_special;
            this.transition = DFA47_transition;
        }
        public String getDescription() {
            return "316:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA47_1 = input.LA(1);

                         
                        int index47_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred68_SJaql()) ) {s = 26;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index47_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 47, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_statement_in_script134 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_25_in_script137 = new BitSet(new long[]{0x06000000000000C0L});
    public static final BitSet FOLLOW_statement_in_script139 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_25_in_script143 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assignment_in_statement155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_statement159 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_assignment175 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_assignment177 = new BitSet(new long[]{0x06000000000000C0L});
    public static final BitSet FOLLOW_operator_in_assignment181 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_contextAwareExpression207 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_expression216 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression229 = new BitSet(new long[]{0x0000000018000002L});
    public static final BitSet FOLLOW_27_in_orExpression233 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_28_in_orExpression237 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_andExpression_in_orExpression242 = new BitSet(new long[]{0x0000000018000002L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression276 = new BitSet(new long[]{0x0000000060000002L});
    public static final BitSet FOLLOW_29_in_andExpression280 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_30_in_andExpression284 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression289 = new BitSet(new long[]{0x0000000060000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression321 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression328 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_elementExpression330 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression334 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression356 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_elementExpression358 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression362 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression389 = new BitSet(new long[]{0x0000007E00000002L});
    public static final BitSet FOLLOW_33_in_comparisonExpression395 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_34_in_comparisonExpression401 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_35_in_comparisonExpression407 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_36_in_comparisonExpression413 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_37_in_comparisonExpression419 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_38_in_comparisonExpression425 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression510 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_39_in_arithmeticExpression516 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_40_in_arithmeticExpression522 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression527 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression570 = new BitSet(new long[]{0x0000020000000102L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression576 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_41_in_multiplicationExpression582 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression587 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_42_in_preincrementExpression628 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression630 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_43_in_preincrementExpression635 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression637 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression642 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unaryExpression652 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_unaryExpression669 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_unaryExpression674 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression700 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_46_in_contextAwarePathExpression711 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression716 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_contextAwarePathExpression734 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression776 = new BitSet(new long[]{0x0080400000000000L});
    public static final BitSet FOLLOW_46_in_pathExpression790 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_pathExpression795 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathExpression813 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression840 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_valueExpression849 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression855 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression861 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression867 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression876 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression882 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_valueExpression888 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression898 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_47_in_parenthesesExpression910 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression912 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_parenthesesExpression914 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionCall935 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_functionCall937 = new BitSet(new long[]{0x06E9BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_functionCall944 = new BitSet(new long[]{0x0003000000000000L});
    public static final BitSet FOLLOW_49_in_functionCall950 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_functionCall954 = new BitSet(new long[]{0x0003000000000000L});
    public static final BitSet FOLLOW_48_in_functionCall964 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment989 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_fieldAssignment991 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment993 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1002 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_46_in_fieldAssignment1004 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1006 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1015 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_fieldAssignment1017 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1019 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_51_in_objectCreation1041 = new BitSet(new long[]{0x00100000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1044 = new BitSet(new long[]{0x0012000000000000L});
    public static final BitSet FOLLOW_49_in_objectCreation1047 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1049 = new BitSet(new long[]{0x0012000000000000L});
    public static final BitSet FOLLOW_49_in_objectCreation1053 = new BitSet(new long[]{0x0010000000000000L});
    public static final BitSet FOLLOW_52_in_objectCreation1058 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_53_in_literal1082 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_54_in_literal1098 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal1114 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal1130 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal1147 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UINT_in_literal1164 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayAccess1183 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1185 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayAccess1187 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayAccess1205 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1210 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1216 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayAccess1219 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayAccess1237 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1242 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1248 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_arrayAccess1251 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1256 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1262 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayAccess1265 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_arrayCreation1290 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1294 = new BitSet(new long[]{0x0102000000000000L});
    public static final BitSet FOLLOW_49_in_arrayCreation1297 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1301 = new BitSet(new long[]{0x0102000000000000L});
    public static final BitSet FOLLOW_49_in_arrayCreation1305 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_arrayCreation1308 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator1343 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator1347 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator1351 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_readOperator1365 = new BitSet(new long[]{0x0000000000000480L});
    public static final BitSet FOLLOW_ID_in_readOperator1370 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_readOperator1375 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_readOperator1381 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_readOperator1383 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_readOperator1387 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_readOperator1389 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_writeOperator1403 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_VAR_in_writeOperator1407 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_writeOperator1409 = new BitSet(new long[]{0x0000000000000480L});
    public static final BitSet FOLLOW_ID_in_writeOperator1414 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1419 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_writeOperator1425 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_writeOperator1427 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1431 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_writeOperator1433 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_genericOperator1454 = new BitSet(new long[]{0x10000000000000C0L});
    public static final BitSet FOLLOW_ID_in_genericOperator1463 = new BitSet(new long[]{0x10000000000000C0L});
    public static final BitSet FOLLOW_operatorFlag_in_genericOperator1474 = new BitSet(new long[]{0x10000000000000C0L});
    public static final BitSet FOLLOW_input_in_genericOperator1477 = new BitSet(new long[]{0x0002000000000082L});
    public static final BitSet FOLLOW_49_in_genericOperator1480 = new BitSet(new long[]{0x10000000000000C0L});
    public static final BitSet FOLLOW_input_in_genericOperator1482 = new BitSet(new long[]{0x0002000000000082L});
    public static final BitSet FOLLOW_operatorOption_in_genericOperator1487 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_ID_in_operatorOption1507 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_ID_in_operatorOption1516 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_contextAwareExpression_in_operatorOption1526 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1547 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1557 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_60_in_input1579 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_VAR_in_input1587 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_input1589 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_VAR_in_input1595 = new BitSet(new long[]{0x0000000000000082L});
    public static final BitSet FOLLOW_ID_in_input1603 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_contextAwareExpression_in_input1612 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred4_SJaql232 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_andExpression_in_synpred4_SJaql242 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred6_SJaql279 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_elementExpression_in_synpred6_SJaql289 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred7_SJaql321 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred8_SJaql328 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_synpred8_SJaql330 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred8_SJaql334 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_33_in_synpred14_SJaql395 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_34_in_synpred14_SJaql401 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_35_in_synpred14_SJaql407 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_36_in_synpred14_SJaql413 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_37_in_synpred14_SJaql419 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_38_in_synpred14_SJaql425 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_synpred14_SJaql430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_39_in_synpred16_SJaql516 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_40_in_synpred16_SJaql522 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_synpred16_SJaql527 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_synpred18_SJaql576 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_41_in_synpred18_SJaql582 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_synpred18_SJaql587 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_synpred23_SJaql669 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_46_in_synpred24_SJaql711 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_synpred24_SJaql716 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred25_SJaql734 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_46_in_synpred26_SJaql790 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_synpred26_SJaql795 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred27_SJaql813 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_synpred28_SJaql776 = new BitSet(new long[]{0x0080400000000000L});
    public static final BitSet FOLLOW_46_in_synpred28_SJaql790 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_ID_in_synpred28_SJaql795 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred28_SJaql813 = new BitSet(new long[]{0x0080400000000002L});
    public static final BitSet FOLLOW_ID_in_synpred60_SJaql1463 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_49_in_synpred62_SJaql1480 = new BitSet(new long[]{0x10000000000000C0L});
    public static final BitSet FOLLOW_input_in_synpred62_SJaql1482 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorOption_in_synpred63_SJaql1487 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred64_SJaql1516 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred65_SJaql1557 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred67_SJaql1587 = new BitSet(new long[]{0x0000000080000000L});
    public static final BitSet FOLLOW_31_in_synpred67_SJaql1589 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred68_SJaql1603 = new BitSet(new long[]{0x06E8BC0000001EC0L});
    public static final BitSet FOLLOW_contextAwareExpression_in_synpred68_SJaql1612 = new BitSet(new long[]{0x0000000000000002L});

}