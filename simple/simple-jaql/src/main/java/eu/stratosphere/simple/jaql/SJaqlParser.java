// $ANTLR 3.3 Nov 30, 2010 12:46:29 /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g 2011-09-30 16:22:09
 
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
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "EXPRESSION", "OPERATOR", "ID", "VAR", "STAR", "DECIMAL", "STRING", "INTEGER", "UINT", "LOWER_LETTER", "UPPER_LETTER", "DIGIT", "SIGN", "COMMENT", "QUOTE", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'using'", "'='", "'or'", "'||'", "'and'", "'&&'", "'not'", "'in'", "'<='", "'>='", "'<'", "'>'", "'=='", "'!='", "'+'", "'-'", "'/'", "'++'", "'--'", "'!'", "'~'", "'.'", "'('", "')'", "','", "':'", "'{'", "'}'", "'true'", "'false'", "'['", "']'", "'read'", "'write'", "'to'", "'preserve'"
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
    public static final int T__61=61;
    public static final int EXPRESSION=4;
    public static final int OPERATOR=5;
    public static final int ID=6;
    public static final int VAR=7;
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
            this.state.ruleMemo = new HashMap[100+1];
             
             
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
    		throw new IllegalArgumentException("Unknown variable " + variable.getText(), new RecognitionException(variable.getInputStream()));
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

                    if ( ((LA1_1>=ID && LA1_1<=VAR)||LA1_1==26||(LA1_1>=58 && LA1_1<=59)) ) {
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:111:1: statement : ( assignment | operator | packageImport ) ->;
    public final SJaqlParser.statement_return statement() throws RecognitionException {
        SJaqlParser.statement_return retval = new SJaqlParser.statement_return();
        retval.start = input.LT(1);
        int statement_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.assignment_return assignment5 = null;

        SJaqlParser.operator_return operator6 = null;

        SJaqlParser.packageImport_return packageImport7 = null;


        RewriteRuleSubtreeStream stream_assignment=new RewriteRuleSubtreeStream(adaptor,"rule assignment");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        RewriteRuleSubtreeStream stream_packageImport=new RewriteRuleSubtreeStream(adaptor,"rule packageImport");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 2) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:2: ( ( assignment | operator | packageImport ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:4: ( assignment | operator | packageImport )
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:4: ( assignment | operator | packageImport )
            int alt2=3;
            switch ( input.LA(1) ) {
            case VAR:
                {
                alt2=1;
                }
                break;
            case ID:
            case 58:
            case 59:
                {
                alt2=2;
                }
                break;
            case 26:
                {
                alt2=3;
                }
                break;
            default:
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
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:112:29: packageImport
                    {
                    pushFollow(FOLLOW_packageImport_in_statement163);
                    packageImport7=packageImport();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_packageImport.add(packageImport7.getTree());

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
            // 112:44: ->
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

    public static class packageImport_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "packageImport"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:114:1: packageImport : 'using' packageName= ID ->;
    public final SJaqlParser.packageImport_return packageImport() throws RecognitionException {
        SJaqlParser.packageImport_return retval = new SJaqlParser.packageImport_return();
        retval.start = input.LT(1);
        int packageImport_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token string_literal8=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression string_literal8_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_26=new RewriteRuleTokenStream(adaptor,"token 26");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 3) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:115:2: ( 'using' packageName= ID ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:115:5: 'using' packageName= ID
            {
            string_literal8=(Token)match(input,26,FOLLOW_26_in_packageImport177); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_26.add(string_literal8);

            packageName=(Token)match(input,ID,FOLLOW_ID_in_packageImport181); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(packageName);

            if ( state.backtracking==0 ) {
               importPackage((packageName!=null?packageName.getText():null)); 
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
            // 115:65: ->
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
            if ( state.backtracking>0 ) { memoize(input, 3, packageImport_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "packageImport"

    public static class assignment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "assignment"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:117:1: assignment : target= VAR '=' source= operator ->;
    public final SJaqlParser.assignment_return assignment() throws RecognitionException {
        SJaqlParser.assignment_return retval = new SJaqlParser.assignment_return();
        retval.start = input.LT(1);
        int assignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token target=null;
        Token char_literal9=null;
        SJaqlParser.operator_return source = null;


        EvaluationExpression target_tree=null;
        EvaluationExpression char_literal9_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_27=new RewriteRuleTokenStream(adaptor,"token 27");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:118:2: (target= VAR '=' source= operator ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:118:4: target= VAR '=' source= operator
            {
            target=(Token)match(input,VAR,FOLLOW_VAR_in_assignment196); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(target);

            char_literal9=(Token)match(input,27,FOLLOW_27_in_assignment198); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_27.add(char_literal9);

            pushFollow(FOLLOW_operator_in_assignment202);
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
            // 118:80: ->
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
            if ( state.backtracking>0 ) { memoize(input, 4, assignment_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:120:1: contextAwareExpression[EvaluationExpression contextExpression] : expression ;
    public final SJaqlParser.contextAwareExpression_return contextAwareExpression(EvaluationExpression contextExpression) throws RecognitionException {
        contextAwareExpression_stack.push(new contextAwareExpression_scope());
        SJaqlParser.contextAwareExpression_return retval = new SJaqlParser.contextAwareExpression_return();
        retval.start = input.LT(1);
        int contextAwareExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.expression_return expression10 = null;



         ((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context = contextExpression; 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 5) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:3: ( expression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:5: expression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_expression_in_contextAwareExpression228);
            expression10=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression10.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 5, contextAwareExpression_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:125:1: expression : orExpression ;
    public final SJaqlParser.expression_return expression() throws RecognitionException {
        SJaqlParser.expression_return retval = new SJaqlParser.expression_return();
        retval.start = input.LT(1);
        int expression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.orExpression_return orExpression11 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:2: ( orExpression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:126:4: orExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_orExpression_in_expression237);
            orExpression11=orExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, orExpression11.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 6, expression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "expression"

    public static class orExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:1: orExpression : exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) ;
    public final SJaqlParser.orExpression_return orExpression() throws RecognitionException {
        SJaqlParser.orExpression_return retval = new SJaqlParser.orExpression_return();
        retval.start = input.LT(1);
        int orExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal12=null;
        Token string_literal13=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal12_tree=null;
        EvaluationExpression string_literal13_tree=null;
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_andExpression=new RewriteRuleSubtreeStream(adaptor,"rule andExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:3: (exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:5: exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression250);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:26: ( ( 'or' | '||' ) exprs+= andExpression )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==28) ) {
                    int LA4_2 = input.LA(2);

                    if ( (synpred5_SJaql()) ) {
                        alt4=1;
                    }


                }
                else if ( (LA4_0==29) ) {
                    int LA4_3 = input.LA(2);

                    if ( (synpred5_SJaql()) ) {
                        alt4=1;
                    }


                }


                switch (alt4) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:27: ( 'or' | '||' ) exprs+= andExpression
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:27: ( 'or' | '||' )
            	    int alt3=2;
            	    int LA3_0 = input.LA(1);

            	    if ( (LA3_0==28) ) {
            	        alt3=1;
            	    }
            	    else if ( (LA3_0==29) ) {
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
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:28: 'or'
            	            {
            	            string_literal12=(Token)match(input,28,FOLLOW_28_in_orExpression254); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_28.add(string_literal12);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:35: '||'
            	            {
            	            string_literal13=(Token)match(input,29,FOLLOW_29_in_orExpression258); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_29.add(string_literal13);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_andExpression_in_orExpression263);
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
            // 130:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 131:3: -> ^( EXPRESSION[\"OrExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:131:6: ^( EXPRESSION[\"OrExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 7, orExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "orExpression"

    public static class andExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "andExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:1: andExpression : exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) ;
    public final SJaqlParser.andExpression_return andExpression() throws RecognitionException {
        SJaqlParser.andExpression_return retval = new SJaqlParser.andExpression_return();
        retval.start = input.LT(1);
        int andExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal14=null;
        Token string_literal15=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal14_tree=null;
        EvaluationExpression string_literal15_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:3: (exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:5: exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression297);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:30: ( ( 'and' | '&&' ) exprs+= elementExpression )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( (LA6_0==30) ) {
                    int LA6_2 = input.LA(2);

                    if ( (synpred7_SJaql()) ) {
                        alt6=1;
                    }


                }
                else if ( (LA6_0==31) ) {
                    int LA6_3 = input.LA(2);

                    if ( (synpred7_SJaql()) ) {
                        alt6=1;
                    }


                }


                switch (alt6) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:31: ( 'and' | '&&' ) exprs+= elementExpression
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:31: ( 'and' | '&&' )
            	    int alt5=2;
            	    int LA5_0 = input.LA(1);

            	    if ( (LA5_0==30) ) {
            	        alt5=1;
            	    }
            	    else if ( (LA5_0==31) ) {
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
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:32: 'and'
            	            {
            	            string_literal14=(Token)match(input,30,FOLLOW_30_in_andExpression301); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_30.add(string_literal14);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:40: '&&'
            	            {
            	            string_literal15=(Token)match(input,31,FOLLOW_31_in_andExpression305); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_31.add(string_literal15);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_elementExpression_in_andExpression310);
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
            // 135:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 136:3: -> ^( EXPRESSION[\"AndExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:136:6: ^( EXPRESSION[\"AndExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 8, andExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "andExpression"

    public static class elementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "elementExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:1: elementExpression : elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) ;
    public final SJaqlParser.elementExpression_return elementExpression() throws RecognitionException {
        SJaqlParser.elementExpression_return retval = new SJaqlParser.elementExpression_return();
        retval.start = input.LT(1);
        int elementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token not=null;
        Token string_literal16=null;
        SJaqlParser.comparisonExpression_return elem = null;

        SJaqlParser.comparisonExpression_return set = null;


        EvaluationExpression not_tree=null;
        EvaluationExpression string_literal16_tree=null;
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 9) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:2: (elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:4: elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )?
            {
            pushFollow(FOLLOW_comparisonExpression_in_elementExpression344);
            elem=comparisonExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:30: ( (not= 'not' )? 'in' set= comparisonExpression )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==32) ) {
                int LA8_1 = input.LA(2);

                if ( (synpred9_SJaql()) ) {
                    alt8=1;
                }
            }
            else if ( (LA8_0==33) ) {
                int LA8_2 = input.LA(2);

                if ( (synpred9_SJaql()) ) {
                    alt8=1;
                }
            }
            switch (alt8) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:31: (not= 'not' )? 'in' set= comparisonExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:34: (not= 'not' )?
                    int alt7=2;
                    int LA7_0 = input.LA(1);

                    if ( (LA7_0==32) ) {
                        alt7=1;
                    }
                    switch (alt7) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: not= 'not'
                            {
                            not=(Token)match(input,32,FOLLOW_32_in_elementExpression349); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_32.add(not);


                            }
                            break;

                    }

                    string_literal16=(Token)match(input,33,FOLLOW_33_in_elementExpression352); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_33.add(string_literal16);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression356);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: elem, elem, set
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
            // 140:2: -> { set == null }? $elem
            if ( set == null ) {
                adaptor.addChild(root_0, stream_elem.nextTree());

            }
            else // 141:2: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:141:5: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ElementInSetExpression"), root_1);

                adaptor.addChild(root_1, stream_elem.nextTree());
                adaptor.addChild(root_1,  not == null ? ElementInSetExpression.Quantor.EXISTS_IN : ElementInSetExpression.Quantor.EXISTS_NOT_IN);
                adaptor.addChild(root_1, stream_set.nextTree());

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
            if ( state.backtracking>0 ) { memoize(input, 9, elementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "elementExpression"

    public static class comparisonExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
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
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleTokenStream stream_37=new RewriteRuleTokenStream(adaptor,"token 37");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_arithmeticExpression=new RewriteRuleSubtreeStream(adaptor,"rule arithmeticExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 10) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:2: (e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:4: e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression397);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:28: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            int alt10=2;
            switch ( input.LA(1) ) {
                case 34:
                    {
                    int LA10_1 = input.LA(2);

                    if ( (synpred15_SJaql()) ) {
                        alt10=1;
                    }
                    }
                    break;
                case 35:
                    {
                    int LA10_2 = input.LA(2);

                    if ( (synpred15_SJaql()) ) {
                        alt10=1;
                    }
                    }
                    break;
                case 36:
                    {
                    int LA10_3 = input.LA(2);

                    if ( (synpred15_SJaql()) ) {
                        alt10=1;
                    }
                    }
                    break;
                case 37:
                    {
                    int LA10_4 = input.LA(2);

                    if ( (synpred15_SJaql()) ) {
                        alt10=1;
                    }
                    }
                    break;
                case 38:
                    {
                    int LA10_5 = input.LA(2);

                    if ( (synpred15_SJaql()) ) {
                        alt10=1;
                    }
                    }
                    break;
                case 39:
                    {
                    int LA10_6 = input.LA(2);

                    if ( (synpred15_SJaql()) ) {
                        alt10=1;
                    }
                    }
                    break;
            }

            switch (alt10) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
                    int alt9=6;
                    switch ( input.LA(1) ) {
                    case 34:
                        {
                        alt9=1;
                        }
                        break;
                    case 35:
                        {
                        alt9=2;
                        }
                        break;
                    case 36:
                        {
                        alt9=3;
                        }
                        break;
                    case 37:
                        {
                        alt9=4;
                        }
                        break;
                    case 38:
                        {
                        alt9=5;
                        }
                        break;
                    case 39:
                        {
                        alt9=6;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 9, 0, input);

                        throw nvae;
                    }

                    switch (alt9) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:30: s= '<='
                            {
                            s=(Token)match(input,34,FOLLOW_34_in_comparisonExpression403); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_34.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:39: s= '>='
                            {
                            s=(Token)match(input,35,FOLLOW_35_in_comparisonExpression409); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_35.add(s);


                            }
                            break;
                        case 3 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:48: s= '<'
                            {
                            s=(Token)match(input,36,FOLLOW_36_in_comparisonExpression415); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_36.add(s);


                            }
                            break;
                        case 4 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:56: s= '>'
                            {
                            s=(Token)match(input,37,FOLLOW_37_in_comparisonExpression421); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_37.add(s);


                            }
                            break;
                        case 5 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:64: s= '=='
                            {
                            s=(Token)match(input,38,FOLLOW_38_in_comparisonExpression427); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_38.add(s);


                            }
                            break;
                        case 6 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:73: s= '!='
                            {
                            s=(Token)match(input,39,FOLLOW_39_in_comparisonExpression433); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_39.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression438);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e2, e1, e1, e2, e1, e1
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
            // 146:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }
            else // 147:3: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:147:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 148:3: -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("==") ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 149:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:149:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
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
            if ( state.backtracking>0 ) { memoize(input, 10, comparisonExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "comparisonExpression"

    public static class arithmeticExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arithmeticExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final SJaqlParser.arithmeticExpression_return arithmeticExpression() throws RecognitionException {
        SJaqlParser.arithmeticExpression_return retval = new SJaqlParser.arithmeticExpression_return();
        retval.start = input.LT(1);
        int arithmeticExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.multiplicationExpression_return e1 = null;

        SJaqlParser.multiplicationExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleSubtreeStream stream_multiplicationExpression=new RewriteRuleSubtreeStream(adaptor,"rule multiplicationExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 11) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:2: (e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:4: e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression518);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:32: ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==40) ) {
                int LA12_1 = input.LA(2);

                if ( (synpred17_SJaql()) ) {
                    alt12=1;
                }
            }
            else if ( (LA12_0==41) ) {
                int LA12_2 = input.LA(2);

                if ( (synpred17_SJaql()) ) {
                    alt12=1;
                }
            }
            switch (alt12) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:33: (s= '+' | s= '-' ) e2= multiplicationExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:33: (s= '+' | s= '-' )
                    int alt11=2;
                    int LA11_0 = input.LA(1);

                    if ( (LA11_0==40) ) {
                        alt11=1;
                    }
                    else if ( (LA11_0==41) ) {
                        alt11=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 11, 0, input);

                        throw nvae;
                    }
                    switch (alt11) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:34: s= '+'
                            {
                            s=(Token)match(input,40,FOLLOW_40_in_arithmeticExpression524); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_40.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:42: s= '-'
                            {
                            s=(Token)match(input,41,FOLLOW_41_in_arithmeticExpression530); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression535);
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
            // 153:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:153:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 155:2: -> $e1
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
            if ( state.backtracking>0 ) { memoize(input, 11, arithmeticExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arithmeticExpression"

    public static class multiplicationExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiplicationExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
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
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleSubtreeStream stream_preincrementExpression=new RewriteRuleSubtreeStream(adaptor,"rule preincrementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:2: (e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:4: e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression578);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:30: ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==STAR) ) {
                int LA14_1 = input.LA(2);

                if ( (synpred19_SJaql()) ) {
                    alt14=1;
                }
            }
            else if ( (LA14_0==42) ) {
                int LA14_2 = input.LA(2);

                if ( (synpred19_SJaql()) ) {
                    alt14=1;
                }
            }
            switch (alt14) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:31: (s= '*' | s= '/' ) e2= preincrementExpression
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:31: (s= '*' | s= '/' )
                    int alt13=2;
                    int LA13_0 = input.LA(1);

                    if ( (LA13_0==STAR) ) {
                        alt13=1;
                    }
                    else if ( (LA13_0==42) ) {
                        alt13=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 13, 0, input);

                        throw nvae;
                    }
                    switch (alt13) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression584); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:40: s= '/'
                            {
                            s=(Token)match(input,42,FOLLOW_42_in_multiplicationExpression590); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_42.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression595);
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
            // 159:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:159:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 161:2: -> $e1
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
            if ( state.backtracking>0 ) { memoize(input, 12, multiplicationExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "multiplicationExpression"

    public static class preincrementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "preincrementExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:163:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 13) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:164:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
            int alt15=3;
            switch ( input.LA(1) ) {
            case 43:
                {
                alt15=1;
                }
                break;
            case 44:
                {
                alt15=2;
                }
                break;
            case ID:
            case VAR:
            case DECIMAL:
            case STRING:
            case INTEGER:
            case UINT:
            case 45:
            case 46:
            case 48:
            case 52:
            case 54:
            case 55:
            case 56:
            case 58:
            case 59:
                {
                alt15=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 15, 0, input);

                throw nvae;
            }

            switch (alt15) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:164:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal17=(Token)match(input,43,FOLLOW_43_in_preincrementExpression636); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal17_tree = (EvaluationExpression)adaptor.create(string_literal17);
                    adaptor.addChild(root_0, string_literal17_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression638);
                    preincrementExpression18=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression18.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:165:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal19=(Token)match(input,44,FOLLOW_44_in_preincrementExpression643); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal19_tree = (EvaluationExpression)adaptor.create(string_literal19);
                    adaptor.addChild(root_0, string_literal19_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression645);
                    preincrementExpression20=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression20.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:166:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression650);
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
            if ( state.backtracking>0 ) { memoize(input, 13, preincrementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "preincrementExpression"

    public static class unaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:1: unaryExpression : ( '!' | '~' )? ( ({...}? contextAwarePathExpression ) | pathExpression ) ;
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:169:2: ( ( '!' | '~' )? ( ({...}? contextAwarePathExpression ) | pathExpression ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:169:4: ( '!' | '~' )? ( ({...}? contextAwarePathExpression ) | pathExpression )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:169:4: ( '!' | '~' )?
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( ((LA16_0>=45 && LA16_0<=46)) ) {
                alt16=1;
            }
            switch (alt16) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:
                    {
                    set22=(Token)input.LT(1);
                    if ( (input.LA(1)>=45 && input.LA(1)<=46) ) {
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

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:4: ( ({...}? contextAwarePathExpression ) | pathExpression )
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==ID) ) {
                switch ( input.LA(2) ) {
                case VAR:
                case 48:
                case 61:
                    {
                    alt17=2;
                    }
                    break;
                case EOF:
                case STAR:
                case 25:
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
                case 42:
                case 47:
                case 49:
                case 50:
                case 53:
                case 56:
                case 57:
                    {
                    alt17=1;
                    }
                    break;
                case ID:
                    {
                    switch ( input.LA(3) ) {
                    case ID:
                        {
                        int LA17_5 = input.LA(4);

                        if ( ((synpred24_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))) ) {
                            alt17=1;
                        }
                        else if ( (true) ) {
                            alt17=2;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 17, 5, input);

                            throw nvae;
                        }
                        }
                        break;
                    case DECIMAL:
                    case STRING:
                    case INTEGER:
                    case UINT:
                    case 43:
                    case 44:
                    case 45:
                    case 46:
                    case 48:
                    case 52:
                    case 54:
                    case 55:
                    case 56:
                    case 58:
                    case 59:
                        {
                        alt17=1;
                        }
                        break;
                    case VAR:
                        {
                        int LA17_6 = input.LA(4);

                        if ( ((synpred24_SJaql()&&(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null))) ) {
                            alt17=1;
                        }
                        else if ( (true) ) {
                            alt17=2;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 17, 6, input);

                            throw nvae;
                        }
                        }
                        break;
                    case 61:
                        {
                        alt17=2;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 17, 4, input);

                        throw nvae;
                    }

                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 17, 1, input);

                    throw nvae;
                }

            }
            else if ( (LA17_0==VAR||(LA17_0>=DECIMAL && LA17_0<=UINT)||LA17_0==48||LA17_0==52||(LA17_0>=54 && LA17_0<=56)||(LA17_0>=58 && LA17_0<=59)) ) {
                alt17=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 17, 0, input);

                throw nvae;
            }
            switch (alt17) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:5: ({...}? contextAwarePathExpression )
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:5: ({...}? contextAwarePathExpression )
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:6: {...}? contextAwarePathExpression
                    {
                    if ( !((((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "unaryExpression", "$contextAwareExpression::context != null");
                    }
                    pushFollow(FOLLOW_contextAwarePathExpression_in_unaryExpression677);
                    contextAwarePathExpression23=contextAwarePathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, contextAwarePathExpression23.getTree());

                    }


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:80: pathExpression
                    {
                    pushFollow(FOLLOW_pathExpression_in_unaryExpression682);
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
            if ( state.backtracking>0 ) { memoize(input, 14, unaryExpression_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:175:1: contextAwarePathExpression : start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) ;
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
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
         ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:3: (start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:5: start= ID ( ( '.' (field= ID ) ) | arrayAccess )*
            {
            start=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression708); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(start);

            if ( state.backtracking==0 ) {
               ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context); ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((start!=null?start.getText():null)));
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:5: ( ( '.' (field= ID ) ) | arrayAccess )*
            loop18:
            do {
                int alt18=3;
                alt18 = dfa18.predict(input);
                switch (alt18) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:7: ( '.' (field= ID ) )
            	    {
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:7: ( '.' (field= ID ) )
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:8: '.' (field= ID )
            	    {
            	    char_literal25=(Token)match(input,47,FOLLOW_47_in_contextAwarePathExpression719); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_47.add(char_literal25);

            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:12: (field= ID )
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:13: field= ID
            	    {
            	    field=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression724); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(field);

            	    if ( state.backtracking==0 ) {
            	       ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
            	    }

            	    }


            	    }


            	    }
            	    break;
            	case 2 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:180:11: arrayAccess
            	    {
            	    pushFollow(FOLLOW_arrayAccess_in_contextAwarePathExpression742);
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
            	    break loop18;
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
            // 180:93: -> ^( EXPRESSION[\"PathExpression\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:180:97: ^( EXPRESSION[\"PathExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 15, contextAwarePathExpression_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:182:1: pathExpression : ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );
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
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
         ((pathExpression_scope)pathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 16) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:3: ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression )
            int alt20=2;
            alt20 = dfa20.predict(input);
            switch (alt20) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:186:5: valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+
                    {
                    pushFollow(FOLLOW_valueExpression_in_pathExpression784);
                    valueExpression27=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(valueExpression27.getTree());
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:5: ( ( '.' (field= ID ) ) | arrayAccess )+
                    int cnt19=0;
                    loop19:
                    do {
                        int alt19=3;
                        int LA19_0 = input.LA(1);

                        if ( (LA19_0==47) ) {
                            int LA19_2 = input.LA(2);

                            if ( (synpred27_SJaql()) ) {
                                alt19=1;
                            }


                        }
                        else if ( (LA19_0==56) ) {
                            int LA19_3 = input.LA(2);

                            if ( (synpred28_SJaql()) ) {
                                alt19=2;
                            }


                        }


                        switch (alt19) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:7: ( '.' (field= ID ) )
                    	    {
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:7: ( '.' (field= ID ) )
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:8: '.' (field= ID )
                    	    {
                    	    char_literal28=(Token)match(input,47,FOLLOW_47_in_pathExpression798); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_47.add(char_literal28);

                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:12: (field= ID )
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:13: field= ID
                    	    {
                    	    field=(Token)match(input,ID,FOLLOW_ID_in_pathExpression803); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(field);

                    	    if ( state.backtracking==0 ) {
                    	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
                    	    }

                    	    }


                    	    }


                    	    }
                    	    break;
                    	case 2 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:11: arrayAccess
                    	    {
                    	    pushFollow(FOLLOW_arrayAccess_in_pathExpression821);
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
                    	    if ( cnt19 >= 1 ) break loop19;
                    	    if (state.backtracking>0) {state.failed=true; return retval;}
                                EarlyExitException eee =
                                    new EarlyExitException(19, input);
                                throw eee;
                        }
                        cnt19++;
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
                    // 189:143: -> ^( EXPRESSION[\"PathExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:147: ^( EXPRESSION[\"PathExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:191:5: valueExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_valueExpression_in_pathExpression848);
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
            if ( state.backtracking>0 ) { memoize(input, 16, pathExpression_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:193:1: valueExpression : ( functionCall | parenthesesExpression | literal | VAR -> | arrayCreation | objectCreation | operatorExpression );
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 17) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:194:2: ( functionCall | parenthesesExpression | literal | VAR -> | arrayCreation | objectCreation | operatorExpression )
            int alt21=7;
            switch ( input.LA(1) ) {
            case ID:
                {
                int LA21_1 = input.LA(2);

                if ( (LA21_1==48) ) {
                    alt21=1;
                }
                else if ( ((LA21_1>=ID && LA21_1<=VAR)||LA21_1==61) ) {
                    alt21=7;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 21, 1, input);

                    throw nvae;
                }
                }
                break;
            case 48:
                {
                alt21=2;
                }
                break;
            case DECIMAL:
            case STRING:
            case INTEGER:
            case UINT:
            case 54:
            case 55:
                {
                alt21=3;
                }
                break;
            case VAR:
                {
                alt21=4;
                }
                break;
            case 56:
                {
                alt21=5;
                }
                break;
            case 52:
                {
                alt21=6;
                }
                break;
            case 58:
            case 59:
                {
                alt21=7;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 21, 0, input);

                throw nvae;
            }

            switch (alt21) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:194:4: functionCall
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_functionCall_in_valueExpression857);
                    functionCall31=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionCall31.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:195:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression863);
                    parenthesesExpression32=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression32.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:196:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_valueExpression869);
                    literal33=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal33.getTree());

                    }
                    break;
                case 4 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:197:4: VAR
                    {
                    VAR34=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression875); if (state.failed) return retval; 
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
                    // 197:8: ->
                    {
                        adaptor.addChild(root_0,  makePath(VAR34) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_arrayCreation_in_valueExpression884);
                    arrayCreation35=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation35.getTree());

                    }
                    break;
                case 6 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_objectCreation_in_valueExpression890);
                    objectCreation36=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation36.getTree());

                    }
                    break;
                case 7 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:4: operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_operatorExpression_in_valueExpression896);
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
            if ( state.backtracking>0 ) { memoize(input, 17, valueExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "valueExpression"

    public static class operatorExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:1: operatorExpression : operator ;
    public final SJaqlParser.operatorExpression_return operatorExpression() throws RecognitionException {
        SJaqlParser.operatorExpression_return retval = new SJaqlParser.operatorExpression_return();
        retval.start = input.LT(1);
        int operatorExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.operator_return operator38 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 18) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:2: ( operator )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:4: operator
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_operator_in_operatorExpression906);
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
            if ( state.backtracking>0 ) { memoize(input, 18, operatorExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "operatorExpression"

    public static class parenthesesExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "parenthesesExpression"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:205:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
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
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 19) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:2: ( ( '(' expression ')' ) -> expression )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:4: ( '(' expression ')' )
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:4: ( '(' expression ')' )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:5: '(' expression ')'
            {
            char_literal39=(Token)match(input,48,FOLLOW_48_in_parenthesesExpression918); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_48.add(char_literal39);

            pushFollow(FOLLOW_expression_in_parenthesesExpression920);
            expression40=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression40.getTree());
            char_literal41=(Token)match(input,49,FOLLOW_49_in_parenthesesExpression922); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_49.add(char_literal41);


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
            // 206:25: -> expression
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
            if ( state.backtracking>0 ) { memoize(input, 19, parenthesesExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "parenthesesExpression"

    public static class functionCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionCall"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:208:1: functionCall : name= ID '(' (param= expression ( ',' param= expression )* )? ')' -> ^( EXPRESSION[\"FunctionCall\"] ) ;
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
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<EvaluationExpression> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 20) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:2: (name= ID '(' (param= expression ( ',' param= expression )* )? ')' -> ^( EXPRESSION[\"FunctionCall\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:4: name= ID '(' (param= expression ( ',' param= expression )* )? ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_functionCall943); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal42=(Token)match(input,48,FOLLOW_48_in_functionCall945); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_48.add(char_literal42);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:211:2: (param= expression ( ',' param= expression )* )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( ((LA23_0>=ID && LA23_0<=VAR)||(LA23_0>=DECIMAL && LA23_0<=UINT)||(LA23_0>=43 && LA23_0<=46)||LA23_0==48||LA23_0==52||(LA23_0>=54 && LA23_0<=56)||(LA23_0>=58 && LA23_0<=59)) ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:211:3: param= expression ( ',' param= expression )*
                    {
                    pushFollow(FOLLOW_expression_in_functionCall952);
                    param=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    if ( state.backtracking==0 ) {
                       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    }
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:212:2: ( ',' param= expression )*
                    loop22:
                    do {
                        int alt22=2;
                        int LA22_0 = input.LA(1);

                        if ( (LA22_0==50) ) {
                            alt22=1;
                        }


                        switch (alt22) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:212:3: ',' param= expression
                    	    {
                    	    char_literal43=(Token)match(input,50,FOLLOW_50_in_functionCall958); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_50.add(char_literal43);

                    	    pushFollow(FOLLOW_expression_in_functionCall962);
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
                    	    break loop22;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal44=(Token)match(input,49,FOLLOW_49_in_functionCall972); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_49.add(char_literal44);



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
            // 213:6: -> ^( EXPRESSION[\"FunctionCall\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:213:9: ^( EXPRESSION[\"FunctionCall\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 20, functionCall_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:215:1: fieldAssignment returns [ObjectCreation.Mapping mapping] : ( VAR '.' STAR -> | VAR '.' ID -> | ID ':' expression ->);
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
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 21) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:216:2: ( VAR '.' STAR -> | VAR '.' ID -> | ID ':' expression ->)
            int alt24=3;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==VAR) ) {
                int LA24_1 = input.LA(2);

                if ( (LA24_1==47) ) {
                    int LA24_3 = input.LA(3);

                    if ( (LA24_3==STAR) ) {
                        alt24=1;
                    }
                    else if ( (LA24_3==ID) ) {
                        alt24=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 24, 3, input);

                        throw nvae;
                    }
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 24, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA24_0==ID) ) {
                alt24=3;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 24, 0, input);

                throw nvae;
            }
            switch (alt24) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:216:4: VAR '.' STAR
                    {
                    VAR45=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment997); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR45);

                    char_literal46=(Token)match(input,47,FOLLOW_47_in_fieldAssignment999); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_47.add(char_literal46);

                    STAR47=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment1001); if (state.failed) return retval; 
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
                    // 216:99: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:217:4: VAR '.' ID
                    {
                    VAR48=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1010); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR48);

                    char_literal49=(Token)match(input,47,FOLLOW_47_in_fieldAssignment1012); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_47.add(char_literal49);

                    ID50=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1014); if (state.failed) return retval; 
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
                    // 217:114: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:218:4: ID ':' expression
                    {
                    ID51=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1023); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID51);

                    char_literal52=(Token)match(input,51,FOLLOW_51_in_fieldAssignment1025); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_51.add(char_literal52);

                    pushFollow(FOLLOW_expression_in_fieldAssignment1027);
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
                    // 218:113: ->
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
            if ( state.backtracking>0 ) { memoize(input, 21, fieldAssignment_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:220:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
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
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 22) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal54=(Token)match(input,52,FOLLOW_52_in_objectCreation1049); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_52.add(char_literal54);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( ((LA27_0>=ID && LA27_0<=VAR)) ) {
                alt27=1;
            }
            switch (alt27) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1052);
                    fieldAssignment55=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment55.getTree());
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:25: ( ',' fieldAssignment )*
                    loop25:
                    do {
                        int alt25=2;
                        int LA25_0 = input.LA(1);

                        if ( (LA25_0==50) ) {
                            int LA25_1 = input.LA(2);

                            if ( ((LA25_1>=ID && LA25_1<=VAR)) ) {
                                alt25=1;
                            }


                        }


                        switch (alt25) {
                    	case 1 :
                    	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:26: ',' fieldAssignment
                    	    {
                    	    char_literal56=(Token)match(input,50,FOLLOW_50_in_objectCreation1055); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_50.add(char_literal56);

                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1057);
                    	    fieldAssignment57=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment57.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop25;
                        }
                    } while (true);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:48: ( ',' )?
                    int alt26=2;
                    int LA26_0 = input.LA(1);

                    if ( (LA26_0==50) ) {
                        alt26=1;
                    }
                    switch (alt26) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                            {
                            char_literal58=(Token)match(input,50,FOLLOW_50_in_objectCreation1061); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_50.add(char_literal58);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal59=(Token)match(input,53,FOLLOW_53_in_objectCreation1066); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_53.add(char_literal59);



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
            // 223:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:62: ^( EXPRESSION[\"ObjectCreation\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 22, objectCreation_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) );
    public final SJaqlParser.literal_return literal() throws RecognitionException {
        SJaqlParser.literal_return retval = new SJaqlParser.literal_return();
        retval.start = input.LT(1);
        int literal_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token val=null;

        EvaluationExpression val_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_DECIMAL=new RewriteRuleTokenStream(adaptor,"token DECIMAL");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_54=new RewriteRuleTokenStream(adaptor,"token 54");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 23) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) )
            int alt28=6;
            switch ( input.LA(1) ) {
            case 54:
                {
                alt28=1;
                }
                break;
            case 55:
                {
                alt28=2;
                }
                break;
            case DECIMAL:
                {
                alt28=3;
                }
                break;
            case STRING:
                {
                alt28=4;
                }
                break;
            case INTEGER:
                {
                alt28=5;
                }
                break;
            case UINT:
                {
                alt28=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 28, 0, input);

                throw nvae;
            }

            switch (alt28) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:4: val= 'true'
                    {
                    val=(Token)match(input,54,FOLLOW_54_in_literal1090); if (state.failed) return retval; 
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
                    // 230:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:18: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:231:4: val= 'false'
                    {
                    val=(Token)match(input,55,FOLLOW_55_in_literal1106); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_55.add(val);



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
                    // 231:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:231:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:232:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal1122); if (state.failed) return retval; 
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
                    // 232:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:232:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal1138); if (state.failed) return retval; 
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
                    // 233:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:233:18: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:234:5: val= INTEGER
                    {
                    val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal1155); if (state.failed) return retval; 
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
                    // 234:17: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:234:20: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:5: val= UINT
                    {
                    val=(Token)match(input,UINT,FOLLOW_UINT_in_literal1172); if (state.failed) return retval; 
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
                    // 235:14: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:235:17: ^( EXPRESSION[\"ConstantExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 23, literal_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "literal"

    public static class arrayAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:1: arrayAccess : ( '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
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
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 24) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:3: ( '[' STAR ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt32=3;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==56) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt32=1;
                    }
                    break;
                case INTEGER:
                    {
                    int LA32_3 = input.LA(3);

                    if ( (LA32_3==57) ) {
                        alt32=2;
                    }
                    else if ( (LA32_3==51) ) {
                        alt32=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 32, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case UINT:
                    {
                    int LA32_4 = input.LA(3);

                    if ( (LA32_4==57) ) {
                        alt32=2;
                    }
                    else if ( (LA32_4==51) ) {
                        alt32=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 32, 4, input);

                        throw nvae;
                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 32, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;
            }
            switch (alt32) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:238:5: '[' STAR ']'
                    {
                    char_literal60=(Token)match(input,56,FOLLOW_56_in_arrayAccess1191); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal60);

                    STAR61=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1193); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR61);

                    char_literal62=(Token)match(input,57,FOLLOW_57_in_arrayAccess1195); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_57.add(char_literal62);



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

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:240:5: '[' (pos= INTEGER | pos= UINT ) ']'
                    {
                    char_literal63=(Token)match(input,56,FOLLOW_56_in_arrayAccess1213); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal63);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:240:9: (pos= INTEGER | pos= UINT )
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
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:240:10: pos= INTEGER
                            {
                            pos=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1218); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(pos);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:240:24: pos= UINT
                            {
                            pos=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1224); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(pos);


                            }
                            break;

                    }

                    char_literal64=(Token)match(input,57,FOLLOW_57_in_arrayAccess1227); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_57.add(char_literal64);



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
                    // 241:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:241:6: ^( EXPRESSION[\"ArrayAccess\"] )
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:5: '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']'
                    {
                    char_literal65=(Token)match(input,56,FOLLOW_56_in_arrayAccess1245); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(char_literal65);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:9: (start= INTEGER | start= UINT )
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
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:10: start= INTEGER
                            {
                            start=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1250); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(start);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:26: start= UINT
                            {
                            start=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1256); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(start);


                            }
                            break;

                    }

                    char_literal66=(Token)match(input,51,FOLLOW_51_in_arrayAccess1259); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_51.add(char_literal66);

                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:42: (end= INTEGER | end= UINT )
                    int alt31=2;
                    int LA31_0 = input.LA(1);

                    if ( (LA31_0==INTEGER) ) {
                        alt31=1;
                    }
                    else if ( (LA31_0==UINT) ) {
                        alt31=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 31, 0, input);

                        throw nvae;
                    }
                    switch (alt31) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:43: end= INTEGER
                            {
                            end=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1264); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(end);


                            }
                            break;
                        case 2 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:242:57: end= UINT
                            {
                            end=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1270); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(end);


                            }
                            break;

                    }

                    char_literal67=(Token)match(input,57,FOLLOW_57_in_arrayAccess1273); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_57.add(char_literal67);



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
                    // 243:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:243:6: ^( EXPRESSION[\"ArrayAccess\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 24, arrayAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayCreation"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:245:1: arrayCreation : '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] $elems) ;
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
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 25) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:2: ( '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] $elems) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:5: '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']'
            {
            char_literal68=(Token)match(input,56,FOLLOW_56_in_arrayCreation1298); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_56.add(char_literal68);

            pushFollow(FOLLOW_expression_in_arrayCreation1302);
            elems=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            if (list_elems==null) list_elems=new ArrayList();
            list_elems.add(elems.getTree());

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:27: ( ',' elems+= expression )*
            loop33:
            do {
                int alt33=2;
                int LA33_0 = input.LA(1);

                if ( (LA33_0==50) ) {
                    int LA33_1 = input.LA(2);

                    if ( ((LA33_1>=ID && LA33_1<=VAR)||(LA33_1>=DECIMAL && LA33_1<=UINT)||(LA33_1>=43 && LA33_1<=46)||LA33_1==48||LA33_1==52||(LA33_1>=54 && LA33_1<=56)||(LA33_1>=58 && LA33_1<=59)) ) {
                        alt33=1;
                    }


                }


                switch (alt33) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:28: ',' elems+= expression
            	    {
            	    char_literal69=(Token)match(input,50,FOLLOW_50_in_arrayCreation1305); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_50.add(char_literal69);

            	    pushFollow(FOLLOW_expression_in_arrayCreation1309);
            	    elems=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            	    if (list_elems==null) list_elems=new ArrayList();
            	    list_elems.add(elems.getTree());


            	    }
            	    break;

            	default :
            	    break loop33;
                }
            } while (true);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:52: ( ',' )?
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( (LA34_0==50) ) {
                alt34=1;
            }
            switch (alt34) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                    {
                    char_literal70=(Token)match(input,50,FOLLOW_50_in_arrayCreation1313); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_50.add(char_literal70);


                    }
                    break;

            }

            char_literal71=(Token)match(input,57,FOLLOW_57_in_arrayCreation1316); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_57.add(char_literal71);



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
            // 246:61: -> ^( EXPRESSION[\"ArrayCreation\"] $elems)
            {
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:64: ^( EXPRESSION[\"ArrayCreation\"] $elems)
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
            if ( state.backtracking>0 ) { memoize(input, 25, arrayCreation_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:248:1: operator returns [Operator op=null] : opRule= ( readOperator | writeOperator | genericOperator ) ;
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 26) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:260:2: (opRule= ( readOperator | writeOperator | genericOperator ) )
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:260:4: opRule= ( readOperator | writeOperator | genericOperator )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:260:11: ( readOperator | writeOperator | genericOperator )
            int alt35=3;
            switch ( input.LA(1) ) {
            case 58:
                {
                alt35=1;
                }
                break;
            case 59:
                {
                alt35=2;
                }
                break;
            case ID:
                {
                alt35=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 35, 0, input);

                throw nvae;
            }

            switch (alt35) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:260:12: readOperator
                    {
                    pushFollow(FOLLOW_readOperator_in_operator1351);
                    readOperator72=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator72.getTree());

                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:260:27: writeOperator
                    {
                    pushFollow(FOLLOW_writeOperator_in_operator1355);
                    writeOperator73=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator73.getTree());

                    }
                    break;
                case 3 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:260:43: genericOperator
                    {
                    pushFollow(FOLLOW_genericOperator_in_operator1359);
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
            if ( state.backtracking>0 ) { memoize(input, 26, operator_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:1: readOperator : 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
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
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 27) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:2: ( 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:4: 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal75=(Token)match(input,58,FOLLOW_58_in_readOperator1373); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_58.add(string_literal75);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:11: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt37=2;
            int LA37_0 = input.LA(1);

            if ( (LA37_0==ID) ) {
                int LA37_1 = input.LA(2);

                if ( (LA37_1==48) ) {
                    alt37=2;
                }
                else if ( (LA37_1==STRING) ) {
                    alt37=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 37, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA37_0==STRING) ) {
                alt37=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 37, 0, input);

                throw nvae;
            }
            switch (alt37) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:12: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:15: (loc= ID )?
                    int alt36=2;
                    int LA36_0 = input.LA(1);

                    if ( (LA36_0==ID) ) {
                        alt36=1;
                    }
                    switch (alt36) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1378); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1383); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:34: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1389); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal76=(Token)match(input,48,FOLLOW_48_in_readOperator1391); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal76);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1395); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal77=(Token)match(input,49,FOLLOW_49_in_readOperator1397); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal77);


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
            // 266:133: ->
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
            if ( state.backtracking>0 ) { memoize(input, 27, readOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "readOperator"

    public static class writeOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "writeOperator"
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:268:1: writeOperator : 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
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
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 28) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:269:2: ( 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:269:4: 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal78=(Token)match(input,59,FOLLOW_59_in_writeOperator1411); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(string_literal78);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator1415); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            string_literal79=(Token)match(input,60,FOLLOW_60_in_writeOperator1417); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_60.add(string_literal79);

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:269:26: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==ID) ) {
                int LA39_1 = input.LA(2);

                if ( (LA39_1==48) ) {
                    alt39=2;
                }
                else if ( (LA39_1==STRING) ) {
                    alt39=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 39, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA39_0==STRING) ) {
                alt39=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 39, 0, input);

                throw nvae;
            }
            switch (alt39) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:269:27: (loc= ID )? file= STRING
                    {
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:269:30: (loc= ID )?
                    int alt38=2;
                    int LA38_0 = input.LA(1);

                    if ( (LA38_0==ID) ) {
                        alt38=1;
                    }
                    switch (alt38) {
                        case 1 :
                            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1422); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1427); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:269:49: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1433); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal80=(Token)match(input,48,FOLLOW_48_in_writeOperator1435); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal80);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1439); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal81=(Token)match(input,49,FOLLOW_49_in_writeOperator1441); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal81);


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
            // 275:3: ->
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
            if ( state.backtracking>0 ) { memoize(input, 28, writeOperator_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:277:1: genericOperator : name= ID ( operatorFlag )* input ( ',' input )* ( operatorOption )* ->;
    public final SJaqlParser.genericOperator_return genericOperator() throws RecognitionException {
        genericOperator_stack.push(new genericOperator_scope());
        SJaqlParser.genericOperator_return retval = new SJaqlParser.genericOperator_return();
        retval.start = input.LT(1);
        int genericOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal84=null;
        SJaqlParser.operatorFlag_return operatorFlag82 = null;

        SJaqlParser.input_return input83 = null;

        SJaqlParser.input_return input85 = null;

        SJaqlParser.operatorOption_return operatorOption86 = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal84_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_operatorOption=new RewriteRuleSubtreeStream(adaptor,"rule operatorOption");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        RewriteRuleSubtreeStream stream_operatorFlag=new RewriteRuleSubtreeStream(adaptor,"rule operatorFlag");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 29) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:280:3: (name= ID ( operatorFlag )* input ( ',' input )* ( operatorOption )* ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:280:5: name= ID ( operatorFlag )* input ( ',' input )* ( operatorOption )*
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1461); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
               ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = findOperatorGreedily(name);
            }
            if ( state.backtracking==0 ) {
               
                if(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo == null)
                  throw new IllegalArgumentException("Unknown operator: " + (name!=null?name.getText():null), new RecognitionException(name.getInputStream()));
                ((operator_scope)operator_stack.peek()).result = ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.newInstance();

            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:286:1: ( operatorFlag )*
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
            	    pushFollow(FOLLOW_operatorFlag_in_genericOperator1468);
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

            pushFollow(FOLLOW_input_in_genericOperator1471);
            input83=input();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_input.add(input83.getTree());
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:7: ( ',' input )*
            loop41:
            do {
                int alt41=2;
                int LA41_0 = input.LA(1);

                if ( (LA41_0==50) ) {
                    int LA41_2 = input.LA(2);

                    if ( (synpred62_SJaql()) ) {
                        alt41=1;
                    }


                }


                switch (alt41) {
            	case 1 :
            	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:8: ',' input
            	    {
            	    char_literal84=(Token)match(input,50,FOLLOW_50_in_genericOperator1474); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_50.add(char_literal84);

            	    pushFollow(FOLLOW_input_in_genericOperator1476);
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

            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:288:1: ( operatorOption )*
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
            	    pushFollow(FOLLOW_operatorOption_in_genericOperator1481);
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
            // 288:17: ->
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
            if ( state.backtracking>0 ) { memoize(input, 29, genericOperator_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:1: operatorOption : name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null] ->;
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 30) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:294:2: (name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null] ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:294:4: name= ID ({...}?moreName= ID )? expr= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1501); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
               ((operatorOption_scope)operatorOption_stack.peek()).optionName = (name!=null?name.getText():null); 
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:295:1: ({...}?moreName= ID )?
            int alt43=2;
            alt43 = dfa43.predict(input);
            switch (alt43) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:295:2: {...}?moreName= ID
                    {
                    if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "operatorOption", "!$genericOperator::operatorInfo.hasProperty($operatorOption::optionName)");
                    }
                    moreName=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1510); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(moreName);

                    if ( state.backtracking==0 ) {
                       ((operatorOption_scope)operatorOption_stack.peek()).optionName = (name!=null?name.getText():null) + " " + (moreName!=null?moreName.getText():null);
                    }

                    }
                    break;

            }

            pushFollow(FOLLOW_contextAwareExpression_in_operatorOption1520);
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
            // 297:143: ->
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
            if ( state.backtracking>0 ) { memoize(input, 30, operatorOption_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:299:1: operatorFlag : name= ID ({...}?moreName= ID )? ->;
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
            if ( state.backtracking>0 && alreadyParsedRule(input, 31) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:303:3: (name= ID ({...}?moreName= ID )? ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:303:5: name= ID ({...}?moreName= ID )?
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1541); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( state.backtracking==0 ) {
               ((operatorFlag_scope)operatorFlag_stack.peek()).flagName = (name!=null?name.getText():null); 
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:1: ({...}?moreName= ID )?
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:2: {...}?moreName= ID
                    {
                    if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "operatorFlag", "!$genericOperator::operatorInfo.hasFlag($operatorFlag::flagName)");
                    }
                    moreName=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1551); if (state.failed) return retval; 
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
            // 306:99: ->
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
            if ( state.backtracking>0 ) { memoize(input, 31, operatorFlag_StartIndex); }
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
    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:308:1: input : (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )? ->;
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
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 32) ) { return retval; }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:2: ( (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )? ->)
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:4: (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?
            {
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:16: (preserveFlag= 'preserve' )?
            int alt45=2;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==61) ) {
                alt45=1;
            }
            switch (alt45) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: preserveFlag= 'preserve'
                    {
                    preserveFlag=(Token)match(input,61,FOLLOW_61_in_input1573); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_61.add(preserveFlag);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:32: (name= VAR 'in' )?
            int alt46=2;
            int LA46_0 = input.LA(1);

            if ( (LA46_0==VAR) ) {
                int LA46_1 = input.LA(2);

                if ( (LA46_1==33) ) {
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
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:33: name= VAR 'in'
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input1581); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);

                    string_literal87=(Token)match(input,33,FOLLOW_33_in_input1583); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_33.add(string_literal87);


                    }
                    break;

            }

            from=(Token)match(input,VAR,FOLLOW_VAR_in_input1589); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                int inputIndex = ((operator_scope)operator_stack.peek()).inputNames.size();
                ((operator_scope)operator_stack.peek()).result.setInput(inputIndex, getVariable(from));
                ((operator_scope)operator_stack.peek()).inputNames.add(name != null ? name.getText() : from.getText());
                ((operator_scope)operator_stack.peek()).hasExplicitName.set(inputIndex, name != null); 
                ((operator_scope)operator_stack.peek()).inputTags.add(preserveFlag == null ? new ArrayList<ExpressionTag>() : Arrays.asList(ExpressionTag.RETAIN));

            }
            // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:317:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?
            int alt47=2;
            alt47 = dfa47.predict(input);
            switch (alt47) {
                case 1 :
                    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:317:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)]
                    {
                    inputOption=(Token)match(input,ID,FOLLOW_ID_in_input1597); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(inputOption);

                    if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasInputProperty((inputOption!=null?inputOption.getText():null)))) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "input", "$genericOperator::operatorInfo.hasInputProperty($inputOption.text)");
                    }
                    pushFollow(FOLLOW_contextAwareExpression_in_input1606);
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
            // 319:1: ->
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
            if ( state.backtracking>0 ) { memoize(input, 32, input_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "input"

    // $ANTLR start synpred5_SJaql
    public final void synpred5_SJaql_fragment() throws RecognitionException {   
        List list_exprs=null;
        RuleReturnScope exprs = null;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:27: ( ( 'or' | '||' ) exprs+= andExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:129:27: ( 'or' | '||' ) exprs+= andExpression
        {
        if ( (input.LA(1)>=28 && input.LA(1)<=29) ) {
            input.consume();
            state.errorRecovery=false;state.failed=false;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            MismatchedSetException mse = new MismatchedSetException(null,input);
            throw mse;
        }

        pushFollow(FOLLOW_andExpression_in_synpred5_SJaql263);
        exprs=andExpression();

        state._fsp--;
        if (state.failed) return ;
        if (list_exprs==null) list_exprs=new ArrayList();
        list_exprs.add(exprs);


        }
    }
    // $ANTLR end synpred5_SJaql

    // $ANTLR start synpred7_SJaql
    public final void synpred7_SJaql_fragment() throws RecognitionException {   
        List list_exprs=null;
        RuleReturnScope exprs = null;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:31: ( ( 'and' | '&&' ) exprs+= elementExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:134:31: ( 'and' | '&&' ) exprs+= elementExpression
        {
        if ( (input.LA(1)>=30 && input.LA(1)<=31) ) {
            input.consume();
            state.errorRecovery=false;state.failed=false;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            MismatchedSetException mse = new MismatchedSetException(null,input);
            throw mse;
        }

        pushFollow(FOLLOW_elementExpression_in_synpred7_SJaql310);
        exprs=elementExpression();

        state._fsp--;
        if (state.failed) return ;
        if (list_exprs==null) list_exprs=new ArrayList();
        list_exprs.add(exprs);


        }
    }
    // $ANTLR end synpred7_SJaql

    // $ANTLR start synpred9_SJaql
    public final void synpred9_SJaql_fragment() throws RecognitionException {   
        Token not=null;
        SJaqlParser.comparisonExpression_return set = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:31: ( (not= 'not' )? 'in' set= comparisonExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:31: (not= 'not' )? 'in' set= comparisonExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:139:34: (not= 'not' )?
        int alt48=2;
        int LA48_0 = input.LA(1);

        if ( (LA48_0==32) ) {
            alt48=1;
        }
        switch (alt48) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: not= 'not'
                {
                not=(Token)match(input,32,FOLLOW_32_in_synpred9_SJaql349); if (state.failed) return ;

                }
                break;

        }

        match(input,33,FOLLOW_33_in_synpred9_SJaql352); if (state.failed) return ;
        pushFollow(FOLLOW_comparisonExpression_in_synpred9_SJaql356);
        set=comparisonExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred9_SJaql

    // $ANTLR start synpred15_SJaql
    public final void synpred15_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.arithmeticExpression_return e2 = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:29: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
        int alt49=6;
        switch ( input.LA(1) ) {
        case 34:
            {
            alt49=1;
            }
            break;
        case 35:
            {
            alt49=2;
            }
            break;
        case 36:
            {
            alt49=3;
            }
            break;
        case 37:
            {
            alt49=4;
            }
            break;
        case 38:
            {
            alt49=5;
            }
            break;
        case 39:
            {
            alt49=6;
            }
            break;
        default:
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 49, 0, input);

            throw nvae;
        }

        switch (alt49) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:30: s= '<='
                {
                s=(Token)match(input,34,FOLLOW_34_in_synpred15_SJaql403); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:39: s= '>='
                {
                s=(Token)match(input,35,FOLLOW_35_in_synpred15_SJaql409); if (state.failed) return ;

                }
                break;
            case 3 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:48: s= '<'
                {
                s=(Token)match(input,36,FOLLOW_36_in_synpred15_SJaql415); if (state.failed) return ;

                }
                break;
            case 4 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:56: s= '>'
                {
                s=(Token)match(input,37,FOLLOW_37_in_synpred15_SJaql421); if (state.failed) return ;

                }
                break;
            case 5 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:64: s= '=='
                {
                s=(Token)match(input,38,FOLLOW_38_in_synpred15_SJaql427); if (state.failed) return ;

                }
                break;
            case 6 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:145:73: s= '!='
                {
                s=(Token)match(input,39,FOLLOW_39_in_synpred15_SJaql433); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_arithmeticExpression_in_synpred15_SJaql438);
        e2=arithmeticExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred15_SJaql

    // $ANTLR start synpred17_SJaql
    public final void synpred17_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.multiplicationExpression_return e2 = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:33: ( (s= '+' | s= '-' ) e2= multiplicationExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:33: (s= '+' | s= '-' ) e2= multiplicationExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:33: (s= '+' | s= '-' )
        int alt50=2;
        int LA50_0 = input.LA(1);

        if ( (LA50_0==40) ) {
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
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:34: s= '+'
                {
                s=(Token)match(input,40,FOLLOW_40_in_synpred17_SJaql524); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:42: s= '-'
                {
                s=(Token)match(input,41,FOLLOW_41_in_synpred17_SJaql530); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_multiplicationExpression_in_synpred17_SJaql535);
        e2=multiplicationExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred17_SJaql

    // $ANTLR start synpred19_SJaql
    public final void synpred19_SJaql_fragment() throws RecognitionException {   
        Token s=null;
        SJaqlParser.preincrementExpression_return e2 = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:31: ( (s= '*' | s= '/' ) e2= preincrementExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:31: (s= '*' | s= '/' ) e2= preincrementExpression
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:31: (s= '*' | s= '/' )
        int alt51=2;
        int LA51_0 = input.LA(1);

        if ( (LA51_0==STAR) ) {
            alt51=1;
        }
        else if ( (LA51_0==42) ) {
            alt51=2;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            NoViableAltException nvae =
                new NoViableAltException("", 51, 0, input);

            throw nvae;
        }
        switch (alt51) {
            case 1 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:32: s= '*'
                {
                s=(Token)match(input,STAR,FOLLOW_STAR_in_synpred19_SJaql584); if (state.failed) return ;

                }
                break;
            case 2 :
                // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:40: s= '/'
                {
                s=(Token)match(input,42,FOLLOW_42_in_synpred19_SJaql590); if (state.failed) return ;

                }
                break;

        }

        pushFollow(FOLLOW_preincrementExpression_in_synpred19_SJaql595);
        e2=preincrementExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred19_SJaql

    // $ANTLR start synpred24_SJaql
    public final void synpred24_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:5: ( ({...}? contextAwarePathExpression ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:5: ({...}? contextAwarePathExpression )
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:5: ({...}? contextAwarePathExpression )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:6: {...}? contextAwarePathExpression
        {
        if ( !((((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context != null)) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred24_SJaql", "$contextAwareExpression::context != null");
        }
        pushFollow(FOLLOW_contextAwarePathExpression_in_synpred24_SJaql677);
        contextAwarePathExpression();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred24_SJaql

    // $ANTLR start synpred25_SJaql
    public final void synpred25_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:7: ( ( '.' (field= ID ) ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:7: ( '.' (field= ID ) )
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:7: ( '.' (field= ID ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:8: '.' (field= ID )
        {
        match(input,47,FOLLOW_47_in_synpred25_SJaql719); if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:12: (field= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred25_SJaql724); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred25_SJaql

    // $ANTLR start synpred26_SJaql
    public final void synpred26_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:180:11: ( arrayAccess )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:180:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred26_SJaql742);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred26_SJaql

    // $ANTLR start synpred27_SJaql
    public final void synpred27_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:7: ( ( '.' (field= ID ) ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:7: ( '.' (field= ID ) )
        {
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:7: ( '.' (field= ID ) )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:8: '.' (field= ID )
        {
        match(input,47,FOLLOW_47_in_synpred27_SJaql798); if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:12: (field= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred27_SJaql803); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred27_SJaql

    // $ANTLR start synpred28_SJaql
    public final void synpred28_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:11: ( arrayAccess )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred28_SJaql821);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred28_SJaql

    // $ANTLR start synpred29_SJaql
    public final void synpred29_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:186:5: ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:186:5: valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+
        {
        pushFollow(FOLLOW_valueExpression_in_synpred29_SJaql784);
        valueExpression();

        state._fsp--;
        if (state.failed) return ;
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:5: ( ( '.' (field= ID ) ) | arrayAccess )+
        int cnt52=0;
        loop52:
        do {
            int alt52=3;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==47) ) {
                alt52=1;
            }
            else if ( (LA52_0==56) ) {
                alt52=2;
            }


            switch (alt52) {
        	case 1 :
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:7: ( '.' (field= ID ) )
        	    {
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:7: ( '.' (field= ID ) )
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:8: '.' (field= ID )
        	    {
        	    match(input,47,FOLLOW_47_in_synpred29_SJaql798); if (state.failed) return ;
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:12: (field= ID )
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:13: field= ID
        	    {
        	    field=(Token)match(input,ID,FOLLOW_ID_in_synpred29_SJaql803); if (state.failed) return ;

        	    }


        	    }


        	    }
        	    break;
        	case 2 :
        	    // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:189:11: arrayAccess
        	    {
        	    pushFollow(FOLLOW_arrayAccess_in_synpred29_SJaql821);
        	    arrayAccess();

        	    state._fsp--;
        	    if (state.failed) return ;

        	    }
        	    break;

        	default :
        	    if ( cnt52 >= 1 ) break loop52;
        	    if (state.backtracking>0) {state.failed=true; return ;}
                    EarlyExitException eee =
                        new EarlyExitException(52, input);
                    throw eee;
            }
            cnt52++;
        } while (true);


        }
    }
    // $ANTLR end synpred29_SJaql

    // $ANTLR start synpred62_SJaql
    public final void synpred62_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:8: ( ',' input )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:8: ',' input
        {
        match(input,50,FOLLOW_50_in_synpred62_SJaql1474); if (state.failed) return ;
        pushFollow(FOLLOW_input_in_synpred62_SJaql1476);
        input();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred62_SJaql

    // $ANTLR start synpred63_SJaql
    public final void synpred63_SJaql_fragment() throws RecognitionException {   
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:288:1: ( operatorOption )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:288:1: operatorOption
        {
        pushFollow(FOLLOW_operatorOption_in_synpred63_SJaql1481);
        operatorOption();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred63_SJaql

    // $ANTLR start synpred64_SJaql
    public final void synpred64_SJaql_fragment() throws RecognitionException {   
        Token moreName=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:295:2: ({...}?moreName= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:295:2: {...}?moreName= ID
        {
        if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasProperty(((operatorOption_scope)operatorOption_stack.peek()).optionName))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred64_SJaql", "!$genericOperator::operatorInfo.hasProperty($operatorOption::optionName)");
        }
        moreName=(Token)match(input,ID,FOLLOW_ID_in_synpred64_SJaql1510); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred64_SJaql

    // $ANTLR start synpred65_SJaql
    public final void synpred65_SJaql_fragment() throws RecognitionException {   
        Token moreName=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:2: ({...}?moreName= ID )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:2: {...}?moreName= ID
        {
        if ( !((!((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasFlag(((operatorFlag_scope)operatorFlag_stack.peek()).flagName))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred65_SJaql", "!$genericOperator::operatorInfo.hasFlag($operatorFlag::flagName)");
        }
        moreName=(Token)match(input,ID,FOLLOW_ID_in_synpred65_SJaql1551); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred65_SJaql

    // $ANTLR start synpred67_SJaql
    public final void synpred67_SJaql_fragment() throws RecognitionException {   
        Token name=null;

        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:33: (name= VAR 'in' )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:33: name= VAR 'in'
        {
        name=(Token)match(input,VAR,FOLLOW_VAR_in_synpred67_SJaql1581); if (state.failed) return ;
        match(input,33,FOLLOW_33_in_synpred67_SJaql1583); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred67_SJaql

    // $ANTLR start synpred68_SJaql
    public final void synpred68_SJaql_fragment() throws RecognitionException {   
        Token inputOption=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:317:2: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )
        // /home/arv/workspace/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:317:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)]
        {
        inputOption=(Token)match(input,ID,FOLLOW_ID_in_synpred68_SJaql1597); if (state.failed) return ;
        if ( !((((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.hasInputProperty((inputOption!=null?inputOption.getText():null)))) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred68_SJaql", "$genericOperator::operatorInfo.hasInputProperty($inputOption.text)");
        }
        pushFollow(FOLLOW_contextAwareExpression_in_synpred68_SJaql1606);
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
    public final boolean synpred5_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred5_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred29_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred29_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred9_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred9_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred15_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred15_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred17_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred17_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred19_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred19_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA18 dfa18 = new DFA18(this);
    protected DFA20 dfa20 = new DFA20(this);
    protected DFA43 dfa43 = new DFA43(this);
    protected DFA47 dfa47 = new DFA47(this);
    static final String DFA18_eotS =
        "\20\uffff";
    static final String DFA18_eofS =
        "\1\1\17\uffff";
    static final String DFA18_minS =
        "\1\6\1\uffff\1\6\1\10\1\0\1\71\2\63\1\uffff\2\0\1\13\1\uffff\2\71"+
        "\1\0";
    static final String DFA18_maxS =
        "\1\71\1\uffff\1\6\1\14\1\0\3\71\1\uffff\2\0\1\14\1\uffff\2\71\1"+
        "\0";
    static final String DFA18_acceptS =
        "\1\uffff\1\3\6\uffff\1\1\3\uffff\1\2\3\uffff";
    static final String DFA18_specialS =
        "\4\uffff\1\3\4\uffff\1\2\1\1\4\uffff\1\0}>";
    static final String[] DFA18_transitionS = {
            "\1\1\1\uffff\1\1\20\uffff\1\1\2\uffff\17\1\4\uffff\1\2\1\uffff"+
            "\2\1\2\uffff\1\1\2\uffff\1\3\1\1",
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
            return "()* loopback of 179:5: ( ( '.' (field= ID ) ) | arrayAccess )*";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA18_15 = input.LA(1);

                         
                        int index18_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred26_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index18_15);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA18_10 = input.LA(1);

                         
                        int index18_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred26_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index18_10);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA18_9 = input.LA(1);

                         
                        int index18_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred26_SJaql()) ) {s = 12;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index18_9);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA18_4 = input.LA(1);

                         
                        int index18_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred25_SJaql()) ) {s = 8;}

                        else if ( (true) ) {s = 1;}

                         
                        input.seek(index18_4);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 18, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA20_eotS =
        "\20\uffff";
    static final String DFA20_eofS =
        "\20\uffff";
    static final String DFA20_minS =
        "\1\6\15\0\2\uffff";
    static final String DFA20_maxS =
        "\1\73\15\0\2\uffff";
    static final String DFA20_acceptS =
        "\16\uffff\1\1\1\2";
    static final String DFA20_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\2\uffff}>";
    static final String[] DFA20_transitionS = {
            "\1\1\1\11\1\uffff\1\5\1\6\1\7\1\10\43\uffff\1\2\3\uffff\1\13"+
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
            return "182:1: pathExpression : ( valueExpression ( ( '.' (field= ID ) ) | arrayAccess )+ -> ^( EXPRESSION[\"PathExpression\"] ) | valueExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA20_1 = input.LA(1);

                         
                        int index20_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA20_2 = input.LA(1);

                         
                        int index20_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA20_3 = input.LA(1);

                         
                        int index20_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA20_4 = input.LA(1);

                         
                        int index20_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA20_5 = input.LA(1);

                         
                        int index20_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA20_6 = input.LA(1);

                         
                        int index20_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA20_7 = input.LA(1);

                         
                        int index20_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA20_8 = input.LA(1);

                         
                        int index20_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA20_9 = input.LA(1);

                         
                        int index20_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA20_10 = input.LA(1);

                         
                        int index20_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA20_11 = input.LA(1);

                         
                        int index20_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA20_12 = input.LA(1);

                         
                        int index20_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA20_13 = input.LA(1);

                         
                        int index20_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred29_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index20_13);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 20, _s, input);
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
        "\1\73\1\0\20\uffff";
    static final String DFA43_acceptS =
        "\2\uffff\1\2\16\uffff\1\1";
    static final String DFA43_specialS =
        "\1\uffff\1\0\20\uffff}>";
    static final String[] DFA43_transitionS = {
            "\1\1\1\2\1\uffff\4\2\36\uffff\4\2\1\uffff\1\2\3\uffff\1\2\1"+
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
            return "295:1: ({...}?moreName= ID )?";
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
        "\1\6\1\0\31\uffff";
    static final String DFA47_maxS =
        "\1\71\1\0\31\uffff";
    static final String DFA47_acceptS =
        "\2\uffff\1\2\27\uffff\1\1";
    static final String DFA47_specialS =
        "\1\uffff\1\0\31\uffff}>";
    static final String[] DFA47_transitionS = {
            "\1\1\1\uffff\1\2\20\uffff\1\2\2\uffff\17\2\4\uffff\1\2\1\uffff"+
            "\2\2\2\uffff\1\2\2\uffff\2\2",
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
            return "317:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::inputNames.size() - 1)] )?";
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
    public static final BitSet FOLLOW_25_in_script137 = new BitSet(new long[]{0x0C000000040000C0L});
    public static final BitSet FOLLOW_statement_in_script139 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_25_in_script143 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assignment_in_statement155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_statement159 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_packageImport_in_statement163 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_26_in_packageImport177 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_packageImport181 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_assignment196 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_27_in_assignment198 = new BitSet(new long[]{0x0C00000000000040L});
    public static final BitSet FOLLOW_operator_in_assignment202 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_contextAwareExpression228 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_expression237 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression250 = new BitSet(new long[]{0x0000000030000002L});
    public static final BitSet FOLLOW_28_in_orExpression254 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_29_in_orExpression258 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_andExpression_in_orExpression263 = new BitSet(new long[]{0x0000000030000002L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression297 = new BitSet(new long[]{0x00000000C0000002L});
    public static final BitSet FOLLOW_30_in_andExpression301 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_31_in_andExpression305 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression310 = new BitSet(new long[]{0x00000000C0000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression344 = new BitSet(new long[]{0x0000000300000002L});
    public static final BitSet FOLLOW_32_in_elementExpression349 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_elementExpression352 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression356 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression397 = new BitSet(new long[]{0x000000FC00000002L});
    public static final BitSet FOLLOW_34_in_comparisonExpression403 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_35_in_comparisonExpression409 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_36_in_comparisonExpression415 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_37_in_comparisonExpression421 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_38_in_comparisonExpression427 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_39_in_comparisonExpression433 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression438 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression518 = new BitSet(new long[]{0x0000030000000002L});
    public static final BitSet FOLLOW_40_in_arithmeticExpression524 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_41_in_arithmeticExpression530 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression535 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression578 = new BitSet(new long[]{0x0000040000000102L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression584 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_42_in_multiplicationExpression590 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression595 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_43_in_preincrementExpression636 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression638 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_44_in_preincrementExpression643 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression645 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression650 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unaryExpression660 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_unaryExpression677 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_unaryExpression682 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression708 = new BitSet(new long[]{0x0100800000000002L});
    public static final BitSet FOLLOW_47_in_contextAwarePathExpression719 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression724 = new BitSet(new long[]{0x0100800000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_contextAwarePathExpression742 = new BitSet(new long[]{0x0100800000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression784 = new BitSet(new long[]{0x0100800000000000L});
    public static final BitSet FOLLOW_47_in_pathExpression798 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_pathExpression803 = new BitSet(new long[]{0x0100800000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathExpression821 = new BitSet(new long[]{0x0100800000000002L});
    public static final BitSet FOLLOW_valueExpression_in_pathExpression848 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_valueExpression857 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression863 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression869 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression875 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression884 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression890 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_valueExpression896 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression906 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_parenthesesExpression918 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression920 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_parenthesesExpression922 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionCall943 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_functionCall945 = new BitSet(new long[]{0x0DD3780000001EC0L});
    public static final BitSet FOLLOW_expression_in_functionCall952 = new BitSet(new long[]{0x0006000000000000L});
    public static final BitSet FOLLOW_50_in_functionCall958 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_expression_in_functionCall962 = new BitSet(new long[]{0x0006000000000000L});
    public static final BitSet FOLLOW_49_in_functionCall972 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment997 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_fieldAssignment999 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment1001 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1010 = new BitSet(new long[]{0x0000800000000000L});
    public static final BitSet FOLLOW_47_in_fieldAssignment1012 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1014 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1023 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_fieldAssignment1025 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1027 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_52_in_objectCreation1049 = new BitSet(new long[]{0x00200000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1052 = new BitSet(new long[]{0x0024000000000000L});
    public static final BitSet FOLLOW_50_in_objectCreation1055 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1057 = new BitSet(new long[]{0x0024000000000000L});
    public static final BitSet FOLLOW_50_in_objectCreation1061 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_objectCreation1066 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_54_in_literal1090 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_55_in_literal1106 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal1122 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal1138 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal1155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UINT_in_literal1172 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_56_in_arrayAccess1191 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1193 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_arrayAccess1195 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_56_in_arrayAccess1213 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1218 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1224 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_arrayAccess1227 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_56_in_arrayAccess1245 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1250 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1256 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_51_in_arrayAccess1259 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1264 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1270 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_arrayAccess1273 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_56_in_arrayCreation1298 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1302 = new BitSet(new long[]{0x0204000000000000L});
    public static final BitSet FOLLOW_50_in_arrayCreation1305 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1309 = new BitSet(new long[]{0x0204000000000000L});
    public static final BitSet FOLLOW_50_in_arrayCreation1313 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_arrayCreation1316 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator1351 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator1355 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator1359 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_readOperator1373 = new BitSet(new long[]{0x0000000000000440L});
    public static final BitSet FOLLOW_ID_in_readOperator1378 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_readOperator1383 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_readOperator1389 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_readOperator1391 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_readOperator1395 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_readOperator1397 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_59_in_writeOperator1411 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_writeOperator1415 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_60_in_writeOperator1417 = new BitSet(new long[]{0x0000000000000440L});
    public static final BitSet FOLLOW_ID_in_writeOperator1422 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1427 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_writeOperator1433 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_writeOperator1435 = new BitSet(new long[]{0x0000000000000400L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1439 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_writeOperator1441 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_genericOperator1461 = new BitSet(new long[]{0x20000000000000C0L});
    public static final BitSet FOLLOW_operatorFlag_in_genericOperator1468 = new BitSet(new long[]{0x20000000000000C0L});
    public static final BitSet FOLLOW_input_in_genericOperator1471 = new BitSet(new long[]{0x0004000000000042L});
    public static final BitSet FOLLOW_50_in_genericOperator1474 = new BitSet(new long[]{0x20000000000000C0L});
    public static final BitSet FOLLOW_input_in_genericOperator1476 = new BitSet(new long[]{0x0004000000000042L});
    public static final BitSet FOLLOW_operatorOption_in_genericOperator1481 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_operatorOption1501 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_ID_in_operatorOption1510 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_contextAwareExpression_in_operatorOption1520 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1541 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1551 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_61_in_input1573 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1581 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_input1583 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1589 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_input1597 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_contextAwareExpression_in_input1606 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred5_SJaql253 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_andExpression_in_synpred5_SJaql263 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_synpred7_SJaql300 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_elementExpression_in_synpred7_SJaql310 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_32_in_synpred9_SJaql349 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_synpred9_SJaql352 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_synpred9_SJaql356 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_34_in_synpred15_SJaql403 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_35_in_synpred15_SJaql409 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_36_in_synpred15_SJaql415 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_37_in_synpred15_SJaql421 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_38_in_synpred15_SJaql427 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_39_in_synpred15_SJaql433 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_synpred15_SJaql438 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_40_in_synpred17_SJaql524 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_41_in_synpred17_SJaql530 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_synpred17_SJaql535 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_synpred19_SJaql584 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_42_in_synpred19_SJaql590 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_synpred19_SJaql595 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_synpred24_SJaql677 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_47_in_synpred25_SJaql719 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred25_SJaql724 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred26_SJaql742 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_47_in_synpred27_SJaql798 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred27_SJaql803 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred28_SJaql821 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_synpred29_SJaql784 = new BitSet(new long[]{0x0100800000000000L});
    public static final BitSet FOLLOW_47_in_synpred29_SJaql798 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred29_SJaql803 = new BitSet(new long[]{0x0100800000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred29_SJaql821 = new BitSet(new long[]{0x0100800000000002L});
    public static final BitSet FOLLOW_50_in_synpred62_SJaql1474 = new BitSet(new long[]{0x20000000000000C0L});
    public static final BitSet FOLLOW_input_in_synpred62_SJaql1476 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorOption_in_synpred63_SJaql1481 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred64_SJaql1510 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred65_SJaql1551 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred67_SJaql1581 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_synpred67_SJaql1583 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred68_SJaql1597 = new BitSet(new long[]{0x0DD1780000001EC0L});
    public static final BitSet FOLLOW_contextAwareExpression_in_synpred68_SJaql1606 = new BitSet(new long[]{0x0000000000000002L});

}