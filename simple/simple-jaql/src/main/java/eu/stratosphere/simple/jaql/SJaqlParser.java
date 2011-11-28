// $ANTLR 3.3 Nov 30, 2010 12:46:29 /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g 2011-11-27 22:41:01
 
package eu.stratosphere.simple.jaql; 

import eu.stratosphere.sopremo.*;
import eu.stratosphere.util.*;
import eu.stratosphere.simple.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.type.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.aggregation.*;
import eu.stratosphere.sopremo.function.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.math.*;
import java.util.IdentityHashMap;
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
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "EXPRESSION", "OPERATOR", "ID", "VAR", "STRING", "STAR", "DECIMAL", "INTEGER", "UINT", "LOWER_LETTER", "UPPER_LETTER", "DIGIT", "SIGN", "COMMENT", "APOSTROPHE", "QUOTATION", "WS", "UNICODE_ESC", "OCTAL_ESC", "ESC_SEQ", "HEX_DIGIT", "EXPONENT", "';'", "'using'", "'='", "'fn'", "'('", "','", "')'", "'javaudf'", "'?'", "':'", "'if'", "'or'", "'||'", "'and'", "'&&'", "'not'", "'in'", "'<='", "'>='", "'<'", "'>'", "'=='", "'!='", "'+'", "'-'", "'/'", "'++'", "'--'", "'!'", "'~'", "'as'", "'.'", "'{'", "'}'", "'true'", "'false'", "'null'", "'['", "']'", "'read'", "'write'", "'to'", "'preserve'"
    };
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


        public SJaqlParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public SJaqlParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
            this.state.ruleMemo = new HashMap[122+1];
             
             
        }
        
    protected TreeAdaptor adaptor = new CommonTreeAdaptor();

    public void setTreeAdaptor(TreeAdaptor adaptor) {
        this.adaptor = adaptor;
    }
    public TreeAdaptor getTreeAdaptor() {
        return adaptor;
    }

    public String[] getTokenNames() { return SJaqlParser.tokenNames; }
    public String getGrammarFileName() { return "/Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g"; }


    {
      addTypeAlias("int", IntNode.class);
      addTypeAlias("decimal", DecimalNode.class);
      addTypeAlias("string", TextNode.class);
      addTypeAlias("double", DoubleNode.class);
      addTypeAlias("boolean", BooleanNode.class);
      addTypeAlias("bool", BooleanNode.class);
      
      addParserFlag(ParserFlag.FUNCTION_OBJECTS);
    }

    public void parseSinks() throws RecognitionException {  
        script();
    }

    private boolean setInnerOutput(Token VAR, Operator<?> op) {
      JsonStreamExpression output = new JsonStreamExpression(((operator_scope)operator_stack.peek()).result.getOutput(((objectCreation_scope)objectCreation_stack.peek()).mappings.size()));
      ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.TagMapping(output, new JsonStreamExpression(op)));
      setBinding(VAR, output, 1);
      return true;
    }

    private EvaluationExpression makePath(Token inputVar, String... path) {
      Object input = getRawBinding(inputVar, Object.class);
    //  if(input == null) {
    //    if(inputVar.getText().equals("$"))
    //      input = ((operator_scope)operator_stack.peek()).numInputs == 1 ? new InputSelection(0) : EvaluationExpression.VALUE;
    //  } else 
      if(input instanceof Operator<?>) {
        int inputIndex = ((operator_scope)operator_stack.peek()).result.getInputs().indexOf(((Operator<?>)input).getSource());
        input = new InputSelection(inputIndex);
      } else if(input instanceof JsonStreamExpression)
        input = ((JsonStreamExpression)input).toInputSelection(((operator_scope)operator_stack.peek()).result);
      
    //    input = new JsonStreamExpression((Operator<?>) input);
      
      List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
      accesses.add((EvaluationExpression) input);
      for (String fragment : path)
        accesses.add(new ObjectAccess(fragment));
      return PathExpression.wrapIfNecessary(accesses);
    }


    public static class script_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "script"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:86:1: script : statement ( ';' statement )* ';' ->;
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
        RewriteRuleTokenStream stream_26=new RewriteRuleTokenStream(adaptor,"token 26");
        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 1) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:87:2: ( statement ( ';' statement )* ';' ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:87:5: statement ( ';' statement )* ';'
            {
            pushFollow(FOLLOW_statement_in_script124);
            statement1=statement();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:87:15: ( ';' statement )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==26) ) {
                    int LA1_1 = input.LA(2);

                    if ( ((LA1_1>=ID && LA1_1<=VAR)||LA1_1==27||(LA1_1>=65 && LA1_1<=66)) ) {
                        alt1=1;
                    }


                }


                switch (alt1) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:87:16: ';' statement
            	    {
            	    char_literal2=(Token)match(input,26,FOLLOW_26_in_script127); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_26.add(char_literal2);

            	    pushFollow(FOLLOW_statement_in_script129);
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

            char_literal4=(Token)match(input,26,FOLLOW_26_in_script133); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_26.add(char_literal4);



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
            // 87:36: ->
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:89:1: statement : ( assignment | operator | packageImport | functionDefinition | javaudf ) ->;
    public final SJaqlParser.statement_return statement() throws RecognitionException {
        SJaqlParser.statement_return retval = new SJaqlParser.statement_return();
        retval.start = input.LT(1);
        int statement_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.assignment_return assignment5 = null;

        SJaqlParser.operator_return operator6 = null;

        SJaqlParser.packageImport_return packageImport7 = null;

        SJaqlParser.functionDefinition_return functionDefinition8 = null;

        SJaqlParser.javaudf_return javaudf9 = null;


        RewriteRuleSubtreeStream stream_assignment=new RewriteRuleSubtreeStream(adaptor,"rule assignment");
        RewriteRuleSubtreeStream stream_functionDefinition=new RewriteRuleSubtreeStream(adaptor,"rule functionDefinition");
        RewriteRuleSubtreeStream stream_javaudf=new RewriteRuleSubtreeStream(adaptor,"rule javaudf");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        RewriteRuleSubtreeStream stream_packageImport=new RewriteRuleSubtreeStream(adaptor,"rule packageImport");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 2) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:2: ( ( assignment | operator | packageImport | functionDefinition | javaudf ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:4: ( assignment | operator | packageImport | functionDefinition | javaudf )
            int alt2=5;
            switch ( input.LA(1) ) {
            case VAR:
                {
                alt2=1;
                }
                break;
            case 65:
            case 66:
                {
                alt2=2;
                }
                break;
            case ID:
                {
                int LA2_3 = input.LA(2);

                if ( (LA2_3==28) ) {
                    int LA2_5 = input.LA(3);

                    if ( (LA2_5==29) ) {
                        alt2=4;
                    }
                    else if ( (LA2_5==33) ) {
                        alt2=5;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 2, 5, input);

                        throw nvae;
                    }
                }
                else if ( ((LA2_3>=ID && LA2_3<=VAR)||LA2_3==63||LA2_3==68) ) {
                    alt2=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 3, input);

                    throw nvae;
                }
                }
                break;
            case 27:
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:5: assignment
                    {
                    pushFollow(FOLLOW_assignment_in_statement145);
                    assignment5=assignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_assignment.add(assignment5.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:18: operator
                    {
                    pushFollow(FOLLOW_operator_in_statement149);
                    operator6=operator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_operator.add(operator6.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:29: packageImport
                    {
                    pushFollow(FOLLOW_packageImport_in_statement153);
                    packageImport7=packageImport();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_packageImport.add(packageImport7.getTree());

                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:45: functionDefinition
                    {
                    pushFollow(FOLLOW_functionDefinition_in_statement157);
                    functionDefinition8=functionDefinition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_functionDefinition.add(functionDefinition8.getTree());

                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:90:66: javaudf
                    {
                    pushFollow(FOLLOW_javaudf_in_statement161);
                    javaudf9=javaudf();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_javaudf.add(javaudf9.getTree());

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
            // 90:75: ->
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:92:1: packageImport : 'using' packageName= ID ->;
    public final SJaqlParser.packageImport_return packageImport() throws RecognitionException {
        SJaqlParser.packageImport_return retval = new SJaqlParser.packageImport_return();
        retval.start = input.LT(1);
        int packageImport_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token string_literal10=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression string_literal10_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_27=new RewriteRuleTokenStream(adaptor,"token 27");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 3) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:93:3: ( 'using' packageName= ID ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:93:6: 'using' packageName= ID
            {
            string_literal10=(Token)match(input,27,FOLLOW_27_in_packageImport176); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_27.add(string_literal10);

            packageName=(Token)match(input,ID,FOLLOW_ID_in_packageImport180); if (state.failed) return retval; 
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
            // 93:66: ->
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:95:1: assignment : target= VAR '=' source= operator ->;
    public final SJaqlParser.assignment_return assignment() throws RecognitionException {
        SJaqlParser.assignment_return retval = new SJaqlParser.assignment_return();
        retval.start = input.LT(1);
        int assignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token target=null;
        Token char_literal11=null;
        SJaqlParser.operator_return source = null;


        EvaluationExpression target_tree=null;
        EvaluationExpression char_literal11_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 4) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:2: (target= VAR '=' source= operator ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:96:4: target= VAR '=' source= operator
            {
            target=(Token)match(input,VAR,FOLLOW_VAR_in_assignment195); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(target);

            char_literal11=(Token)match(input,28,FOLLOW_28_in_assignment197); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal11);

            pushFollow(FOLLOW_operator_in_assignment201);
            source=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(source.getTree());
            if ( state.backtracking==0 ) {
               setBinding(target, new JsonStreamExpression((source!=null?source.op:null))); 
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
            // 96:98: ->
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

    public static class functionDefinition_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "functionDefinition"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:98:1: functionDefinition : name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->;
    public final SJaqlParser.functionDefinition_return functionDefinition() throws RecognitionException {
        SJaqlParser.functionDefinition_return retval = new SJaqlParser.functionDefinition_return();
        retval.start = input.LT(1);
        int functionDefinition_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token param=null;
        Token char_literal12=null;
        Token string_literal13=null;
        Token char_literal14=null;
        Token char_literal15=null;
        Token char_literal16=null;
        SJaqlParser.contextAwareExpression_return def = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression param_tree=null;
        EvaluationExpression char_literal12_tree=null;
        EvaluationExpression string_literal13_tree=null;
        EvaluationExpression char_literal14_tree=null;
        EvaluationExpression char_literal15_tree=null;
        EvaluationExpression char_literal16_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleTokenStream stream_29=new RewriteRuleTokenStream(adaptor,"token 29");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
         List<Token> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 5) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:100:3: (name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null] ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:100:5: name= ID '=' 'fn' '(' (param= ID ( ',' param= ID )* )? ')' def= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition223); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal12=(Token)match(input,28,FOLLOW_28_in_functionDefinition225); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal12);

            string_literal13=(Token)match(input,29,FOLLOW_29_in_functionDefinition227); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_29.add(string_literal13);

            char_literal14=(Token)match(input,30,FOLLOW_30_in_functionDefinition229); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal14);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:101:3: (param= ID ( ',' param= ID )* )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==ID) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:101:4: param= ID ( ',' param= ID )*
                    {
                    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition238); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(param);

                    if ( state.backtracking==0 ) {
                       params.add(param); 
                    }
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:102:3: ( ',' param= ID )*
                    loop3:
                    do {
                        int alt3=2;
                        int LA3_0 = input.LA(1);

                        if ( (LA3_0==31) ) {
                            alt3=1;
                        }


                        switch (alt3) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:102:4: ',' param= ID
                    	    {
                    	    char_literal15=(Token)match(input,31,FOLLOW_31_in_functionDefinition245); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal15);

                    	    param=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition249); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(param);

                    	    if ( state.backtracking==0 ) {
                    	       params.add(param); 
                    	    }

                    	    }
                    	    break;

                    	default :
                    	    break loop3;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal16=(Token)match(input,32,FOLLOW_32_in_functionDefinition260); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal16);

            if ( state.backtracking==0 ) {
               for(int index = 0; index < params.size(); index++) setBinding(params.get(index), new InputSelection(0)); 
            }
            pushFollow(FOLLOW_contextAwareExpression_in_functionDefinition272);
            def=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(def.getTree());
            if ( state.backtracking==0 ) {
               addFunction(new SopremoFunction(name.getText(), def.tree)); 
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
            // 105:100: ->
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
            if ( state.backtracking>0 ) { memoize(input, 5, functionDefinition_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "functionDefinition"

    public static class javaudf_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "javaudf"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:107:1: javaudf : name= ID '=' 'javaudf' '(' path= STRING ')' ->;
    public final SJaqlParser.javaudf_return javaudf() throws RecognitionException {
        SJaqlParser.javaudf_return retval = new SJaqlParser.javaudf_return();
        retval.start = input.LT(1);
        int javaudf_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token path=null;
        Token char_literal17=null;
        Token string_literal18=null;
        Token char_literal19=null;
        Token char_literal20=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression path_tree=null;
        EvaluationExpression char_literal17_tree=null;
        EvaluationExpression string_literal18_tree=null;
        EvaluationExpression char_literal19_tree=null;
        EvaluationExpression char_literal20_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 6) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:108:3: (name= ID '=' 'javaudf' '(' path= STRING ')' ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:108:5: name= ID '=' 'javaudf' '(' path= STRING ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_javaudf290); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal17=(Token)match(input,28,FOLLOW_28_in_javaudf292); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_28.add(char_literal17);

            string_literal18=(Token)match(input,33,FOLLOW_33_in_javaudf294); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_33.add(string_literal18);

            char_literal19=(Token)match(input,30,FOLLOW_30_in_javaudf296); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal19);

            path=(Token)match(input,STRING,FOLLOW_STRING_in_javaudf300); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_STRING.add(path);

            char_literal20=(Token)match(input,32,FOLLOW_32_in_javaudf302); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal20);

            if ( state.backtracking==0 ) {
               addFunction(name.getText(), path.getText()); 
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
            // 109:53: ->
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
            if ( state.backtracking>0 ) { memoize(input, 6, javaudf_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "javaudf"

    protected static class contextAwareExpression_scope {
        EvaluationExpression context;
    }
    protected Stack contextAwareExpression_stack = new Stack();

    public static class contextAwareExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwareExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:111:1: contextAwareExpression[EvaluationExpression contextExpression] : expression ;
    public final SJaqlParser.contextAwareExpression_return contextAwareExpression(EvaluationExpression contextExpression) throws RecognitionException {
        contextAwareExpression_stack.push(new contextAwareExpression_scope());
        SJaqlParser.contextAwareExpression_return retval = new SJaqlParser.contextAwareExpression_return();
        retval.start = input.LT(1);
        int contextAwareExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.expression_return expression21 = null;



         ((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context = contextExpression; 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 7) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:114:3: ( expression )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:114:5: expression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            pushFollow(FOLLOW_expression_in_contextAwareExpression330);
            expression21=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expression21.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 7, contextAwareExpression_StartIndex); }
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:116:1: expression : ( ternaryExpression | operatorExpression );
    public final SJaqlParser.expression_return expression() throws RecognitionException {
        SJaqlParser.expression_return retval = new SJaqlParser.expression_return();
        retval.start = input.LT(1);
        int expression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.ternaryExpression_return ternaryExpression22 = null;

        SJaqlParser.operatorExpression_return operatorExpression23 = null;



        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 8) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:117:3: ( ternaryExpression | operatorExpression )
            int alt5=2;
            alt5 = dfa5.predict(input);
            switch (alt5) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:117:5: ternaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_ternaryExpression_in_expression340);
                    ternaryExpression22=ternaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, ternaryExpression22.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:118:5: operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_operatorExpression_in_expression346);
                    operatorExpression23=operatorExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operatorExpression23.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 8, expression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "expression"

    public static class ternaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "ternaryExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:120:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );
    public final SJaqlParser.ternaryExpression_return ternaryExpression() throws RecognitionException {
        SJaqlParser.ternaryExpression_return retval = new SJaqlParser.ternaryExpression_return();
        retval.start = input.LT(1);
        int ternaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal24=null;
        Token char_literal25=null;
        Token string_literal26=null;
        SJaqlParser.orExpression_return ifClause = null;

        SJaqlParser.expression_return ifExpr = null;

        SJaqlParser.expression_return elseExpr = null;

        SJaqlParser.orExpression_return ifExpr2 = null;

        SJaqlParser.expression_return ifClause2 = null;

        SJaqlParser.orExpression_return orExpression27 = null;


        EvaluationExpression char_literal24_tree=null;
        EvaluationExpression char_literal25_tree=null;
        EvaluationExpression string_literal26_tree=null;
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_36=new RewriteRuleTokenStream(adaptor,"token 36");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_orExpression=new RewriteRuleSubtreeStream(adaptor,"rule orExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 9) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:2: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression )
            int alt7=3;
            alt7 = dfa7.predict(input);
            switch (alt7) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression357);
                    ifClause=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifClause.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:27: '?' (ifExpr= expression )? ':' elseExpr= expression
                    {
                    char_literal24=(Token)match(input,34,FOLLOW_34_in_ternaryExpression360); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_34.add(char_literal24);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:37: (ifExpr= expression )?
                    int alt6=2;
                    int LA6_0 = input.LA(1);

                    if ( ((LA6_0>=ID && LA6_0<=STRING)||(LA6_0>=DECIMAL && LA6_0<=UINT)||LA6_0==30||(LA6_0>=52 && LA6_0<=55)||LA6_0==58||(LA6_0>=60 && LA6_0<=63)||(LA6_0>=65 && LA6_0<=66)) ) {
                        alt6=1;
                    }
                    switch (alt6) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ifExpr= expression
                            {
                            pushFollow(FOLLOW_expression_in_ternaryExpression364);
                            ifExpr=expression();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_expression.add(ifExpr.getTree());

                            }
                            break;

                    }

                    char_literal25=(Token)match(input,35,FOLLOW_35_in_ternaryExpression367); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal25);

                    pushFollow(FOLLOW_expression_in_ternaryExpression371);
                    elseExpr=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(elseExpr.getTree());

                    }



                    // AST REWRITE
                    // elements: ifClause
                    // token labels: 
                    // rule labels: retval, ifClause
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause=new RewriteRuleSubtreeStream(adaptor,"rule ifClause",ifClause!=null?ifClause.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 122:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:122:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression"), root_1);

                        adaptor.addChild(root_1, stream_ifClause.nextTree());
                        adaptor.addChild(root_1,  ifExpr == null ? EvaluationExpression.VALUE : (ifExpr!=null?((EvaluationExpression)ifExpr.tree):null) );
                        adaptor.addChild(root_1,  (elseExpr!=null?((EvaluationExpression)elseExpr.tree):null) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:4: ifExpr2= orExpression 'if' ifClause2= expression
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression394);
                    ifExpr2=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifExpr2.getTree());
                    string_literal26=(Token)match(input,36,FOLLOW_36_in_ternaryExpression396); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_36.add(string_literal26);

                    pushFollow(FOLLOW_expression_in_ternaryExpression400);
                    ifClause2=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(ifClause2.getTree());


                    // AST REWRITE
                    // elements: ifExpr2, ifClause2
                    // token labels: 
                    // rule labels: retval, ifExpr2, ifClause2
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifExpr2=new RewriteRuleSubtreeStream(adaptor,"rule ifExpr2",ifExpr2!=null?ifExpr2.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause2=new RewriteRuleSubtreeStream(adaptor,"rule ifClause2",ifClause2!=null?ifClause2.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 124:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:124:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression"), root_1);

                        adaptor.addChild(root_1, stream_ifClause2.nextTree());
                        adaptor.addChild(root_1, stream_ifExpr2.nextTree());
                        adaptor.addChild(root_1,  EvaluationExpression.VALUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:125:5: orExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_orExpression_in_ternaryExpression422);
                    orExpression27=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, orExpression27.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 9, ternaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "ternaryExpression"

    public static class orExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "orExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:127:1: orExpression : exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) ;
    public final SJaqlParser.orExpression_return orExpression() throws RecognitionException {
        SJaqlParser.orExpression_return retval = new SJaqlParser.orExpression_return();
        retval.start = input.LT(1);
        int orExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal28=null;
        Token string_literal29=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal28_tree=null;
        EvaluationExpression string_literal29_tree=null;
        RewriteRuleTokenStream stream_37=new RewriteRuleTokenStream(adaptor,"token 37");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_andExpression=new RewriteRuleSubtreeStream(adaptor,"rule andExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 10) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:3: (exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"OrExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:5: exprs+= andExpression ( ( 'or' | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression435);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:26: ( ( 'or' | '||' ) exprs+= andExpression )*
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( ((LA9_0>=37 && LA9_0<=38)) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:27: ( 'or' | '||' ) exprs+= andExpression
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:27: ( 'or' | '||' )
            	    int alt8=2;
            	    int LA8_0 = input.LA(1);

            	    if ( (LA8_0==37) ) {
            	        alt8=1;
            	    }
            	    else if ( (LA8_0==38) ) {
            	        alt8=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 8, 0, input);

            	        throw nvae;
            	    }
            	    switch (alt8) {
            	        case 1 :
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:28: 'or'
            	            {
            	            string_literal28=(Token)match(input,37,FOLLOW_37_in_orExpression439); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_37.add(string_literal28);


            	            }
            	            break;
            	        case 2 :
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:128:35: '||'
            	            {
            	            string_literal29=(Token)match(input,38,FOLLOW_38_in_orExpression443); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_38.add(string_literal29);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_andExpression_in_orExpression448);
            	    exprs=andExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop9;
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
            // 129:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 130:3: -> ^( EXPRESSION[\"OrExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:130:6: ^( EXPRESSION[\"OrExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 10, orExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "orExpression"

    public static class andExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "andExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:132:1: andExpression : exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) ;
    public final SJaqlParser.andExpression_return andExpression() throws RecognitionException {
        SJaqlParser.andExpression_return retval = new SJaqlParser.andExpression_return();
        retval.start = input.LT(1);
        int andExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal30=null;
        Token string_literal31=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression string_literal30_tree=null;
        EvaluationExpression string_literal31_tree=null;
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 11) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:3: (exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? -> ^( EXPRESSION[\"AndExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:5: exprs+= elementExpression ( ( 'and' | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression482);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:30: ( ( 'and' | '&&' ) exprs+= elementExpression )*
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( ((LA11_0>=39 && LA11_0<=40)) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:31: ( 'and' | '&&' ) exprs+= elementExpression
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:31: ( 'and' | '&&' )
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
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:32: 'and'
            	            {
            	            string_literal30=(Token)match(input,39,FOLLOW_39_in_andExpression486); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_39.add(string_literal30);


            	            }
            	            break;
            	        case 2 :
            	            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:133:40: '&&'
            	            {
            	            string_literal31=(Token)match(input,40,FOLLOW_40_in_andExpression490); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_40.add(string_literal31);


            	            }
            	            break;

            	    }

            	    pushFollow(FOLLOW_elementExpression_in_andExpression495);
            	    exprs=elementExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop11;
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
            // 134:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }
            else // 135:3: -> ^( EXPRESSION[\"AndExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:135:6: ^( EXPRESSION[\"AndExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 11, andExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "andExpression"

    public static class elementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "elementExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:137:1: elementExpression : elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) ;
    public final SJaqlParser.elementExpression_return elementExpression() throws RecognitionException {
        SJaqlParser.elementExpression_return retval = new SJaqlParser.elementExpression_return();
        retval.start = input.LT(1);
        int elementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token not=null;
        Token string_literal32=null;
        SJaqlParser.comparisonExpression_return elem = null;

        SJaqlParser.comparisonExpression_return set = null;


        EvaluationExpression not_tree=null;
        EvaluationExpression string_literal32_tree=null;
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 12) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:2: (elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:4: elem= comparisonExpression ( (not= 'not' )? 'in' set= comparisonExpression )?
            {
            pushFollow(FOLLOW_comparisonExpression_in_elementExpression529);
            elem=comparisonExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:30: ( (not= 'not' )? 'in' set= comparisonExpression )?
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( ((LA13_0>=41 && LA13_0<=42)) ) {
                alt13=1;
            }
            switch (alt13) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:31: (not= 'not' )? 'in' set= comparisonExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:138:34: (not= 'not' )?
                    int alt12=2;
                    int LA12_0 = input.LA(1);

                    if ( (LA12_0==41) ) {
                        alt12=1;
                    }
                    switch (alt12) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: not= 'not'
                            {
                            not=(Token)match(input,41,FOLLOW_41_in_elementExpression534); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(not);


                            }
                            break;

                    }

                    string_literal32=(Token)match(input,42,FOLLOW_42_in_elementExpression537); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal32);

                    pushFollow(FOLLOW_comparisonExpression_in_elementExpression541);
                    set=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(set.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: elem, set, elem
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
            // 139:2: -> { set == null }? $elem
            if ( set == null ) {
                adaptor.addChild(root_0, stream_elem.nextTree());

            }
            else // 140:2: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:140:5: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
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
            if ( state.backtracking>0 ) { memoize(input, 12, elementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "elementExpression"

    public static class comparisonExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "comparisonExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:143:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
    public final SJaqlParser.comparisonExpression_return comparisonExpression() throws RecognitionException {
        SJaqlParser.comparisonExpression_return retval = new SJaqlParser.comparisonExpression_return();
        retval.start = input.LT(1);
        int comparisonExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.arithmeticExpression_return e1 = null;

        SJaqlParser.arithmeticExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_44=new RewriteRuleTokenStream(adaptor,"token 44");
        RewriteRuleTokenStream stream_47=new RewriteRuleTokenStream(adaptor,"token 47");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_arithmeticExpression=new RewriteRuleSubtreeStream(adaptor,"rule arithmeticExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 13) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:2: (e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:4: e1= arithmeticExpression ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression582);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:28: ( (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression )?
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( ((LA15_0>=43 && LA15_0<=48)) ) {
                alt15=1;
            }
            switch (alt15) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' ) e2= arithmeticExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:29: (s= '<=' | s= '>=' | s= '<' | s= '>' | s= '==' | s= '!=' )
                    int alt14=6;
                    switch ( input.LA(1) ) {
                    case 43:
                        {
                        alt14=1;
                        }
                        break;
                    case 44:
                        {
                        alt14=2;
                        }
                        break;
                    case 45:
                        {
                        alt14=3;
                        }
                        break;
                    case 46:
                        {
                        alt14=4;
                        }
                        break;
                    case 47:
                        {
                        alt14=5;
                        }
                        break;
                    case 48:
                        {
                        alt14=6;
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
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:30: s= '<='
                            {
                            s=(Token)match(input,43,FOLLOW_43_in_comparisonExpression588); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_43.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:39: s= '>='
                            {
                            s=(Token)match(input,44,FOLLOW_44_in_comparisonExpression594); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_44.add(s);


                            }
                            break;
                        case 3 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:48: s= '<'
                            {
                            s=(Token)match(input,45,FOLLOW_45_in_comparisonExpression600); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_45.add(s);


                            }
                            break;
                        case 4 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:56: s= '>'
                            {
                            s=(Token)match(input,46,FOLLOW_46_in_comparisonExpression606); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_46.add(s);


                            }
                            break;
                        case 5 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:64: s= '=='
                            {
                            s=(Token)match(input,47,FOLLOW_47_in_comparisonExpression612); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_47.add(s);


                            }
                            break;
                        case 6 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:144:73: s= '!='
                            {
                            s=(Token)match(input,48,FOLLOW_48_in_comparisonExpression618); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_48.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression623);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: e2, e1, e2, e1, e1, e2, e1
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
            // 145:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }
            else // 146:3: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:146:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 147:3: -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("==") ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:147:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.EQUAL);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 148:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:148:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
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
            if ( state.backtracking>0 ) { memoize(input, 13, comparisonExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "comparisonExpression"

    public static class arithmeticExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arithmeticExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:150:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final SJaqlParser.arithmeticExpression_return arithmeticExpression() throws RecognitionException {
        SJaqlParser.arithmeticExpression_return retval = new SJaqlParser.arithmeticExpression_return();
        retval.start = input.LT(1);
        int arithmeticExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token s=null;
        SJaqlParser.multiplicationExpression_return e1 = null;

        SJaqlParser.multiplicationExpression_return e2 = null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_multiplicationExpression=new RewriteRuleSubtreeStream(adaptor,"rule multiplicationExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 14) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:2: (e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:4: e1= multiplicationExpression ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression703);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:32: ( (s= '+' | s= '-' ) e2= multiplicationExpression )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( ((LA17_0>=49 && LA17_0<=50)) ) {
                alt17=1;
            }
            switch (alt17) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:33: (s= '+' | s= '-' ) e2= multiplicationExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:33: (s= '+' | s= '-' )
                    int alt16=2;
                    int LA16_0 = input.LA(1);

                    if ( (LA16_0==49) ) {
                        alt16=1;
                    }
                    else if ( (LA16_0==50) ) {
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
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:34: s= '+'
                            {
                            s=(Token)match(input,49,FOLLOW_49_in_arithmeticExpression709); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:151:42: s= '-'
                            {
                            s=(Token)match(input,50,FOLLOW_50_in_arithmeticExpression715); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_50.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression720);
                    e2=multiplicationExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_multiplicationExpression.add(e2.getTree());

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
            // 152:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:152:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 154:2: -> $e1
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
            if ( state.backtracking>0 ) { memoize(input, 14, arithmeticExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arithmeticExpression"

    public static class multiplicationExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "multiplicationExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:156:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
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
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleSubtreeStream stream_preincrementExpression=new RewriteRuleSubtreeStream(adaptor,"rule preincrementExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 15) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:2: (e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:4: e1= preincrementExpression ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression763);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:30: ( (s= '*' | s= '/' ) e2= preincrementExpression )?
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==STAR||LA19_0==51) ) {
                alt19=1;
            }
            switch (alt19) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:31: (s= '*' | s= '/' ) e2= preincrementExpression
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:31: (s= '*' | s= '/' )
                    int alt18=2;
                    int LA18_0 = input.LA(1);

                    if ( (LA18_0==STAR) ) {
                        alt18=1;
                    }
                    else if ( (LA18_0==51) ) {
                        alt18=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 18, 0, input);

                        throw nvae;
                    }
                    switch (alt18) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression769); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:157:40: s= '/'
                            {
                            s=(Token)match(input,51,FOLLOW_51_in_multiplicationExpression775); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_51.add(s);


                            }
                            break;

                    }

                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression780);
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
            // 158:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:158:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression"), root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());
                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);
                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }
            else // 160:2: -> $e1
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
            if ( state.backtracking>0 ) { memoize(input, 15, multiplicationExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "multiplicationExpression"

    public static class preincrementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "preincrementExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:162:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
    public final SJaqlParser.preincrementExpression_return preincrementExpression() throws RecognitionException {
        SJaqlParser.preincrementExpression_return retval = new SJaqlParser.preincrementExpression_return();
        retval.start = input.LT(1);
        int preincrementExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token string_literal33=null;
        Token string_literal35=null;
        SJaqlParser.preincrementExpression_return preincrementExpression34 = null;

        SJaqlParser.preincrementExpression_return preincrementExpression36 = null;

        SJaqlParser.unaryExpression_return unaryExpression37 = null;


        EvaluationExpression string_literal33_tree=null;
        EvaluationExpression string_literal35_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 16) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:163:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
            int alt20=3;
            switch ( input.LA(1) ) {
            case 52:
                {
                alt20=1;
                }
                break;
            case 53:
                {
                alt20=2;
                }
                break;
            case ID:
            case VAR:
            case STRING:
            case DECIMAL:
            case INTEGER:
            case UINT:
            case 30:
            case 54:
            case 55:
            case 58:
            case 60:
            case 61:
            case 62:
            case 63:
                {
                alt20=3;
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:163:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal33=(Token)match(input,52,FOLLOW_52_in_preincrementExpression821); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal33_tree = (EvaluationExpression)adaptor.create(string_literal33);
                    adaptor.addChild(root_0, string_literal33_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression823);
                    preincrementExpression34=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression34.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:164:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    string_literal35=(Token)match(input,53,FOLLOW_53_in_preincrementExpression828); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal35_tree = (EvaluationExpression)adaptor.create(string_literal35);
                    adaptor.addChild(root_0, string_literal35_tree);
                    }
                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression830);
                    preincrementExpression36=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression36.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:165:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression835);
                    unaryExpression37=unaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression37.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 16, preincrementExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "preincrementExpression"

    public static class unaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "unaryExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:167:1: unaryExpression : ( '!' | '~' )? castExpression ;
    public final SJaqlParser.unaryExpression_return unaryExpression() throws RecognitionException {
        SJaqlParser.unaryExpression_return retval = new SJaqlParser.unaryExpression_return();
        retval.start = input.LT(1);
        int unaryExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token set38=null;
        SJaqlParser.castExpression_return castExpression39 = null;


        EvaluationExpression set38_tree=null;

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 17) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:2: ( ( '!' | '~' )? castExpression )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:4: ( '!' | '~' )? castExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:168:4: ( '!' | '~' )?
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( ((LA21_0>=54 && LA21_0<=55)) ) {
                alt21=1;
            }
            switch (alt21) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:
                    {
                    set38=(Token)input.LT(1);
                    if ( (input.LA(1)>=54 && input.LA(1)<=55) ) {
                        input.consume();
                        if ( state.backtracking==0 ) adaptor.addChild(root_0, (EvaluationExpression)adaptor.create(set38));
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

            pushFollow(FOLLOW_castExpression_in_unaryExpression854);
            castExpression39=castExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, castExpression39.getTree());

            }

            retval.stop = input.LT(-1);

            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 17, unaryExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "unaryExpression"

    public static class castExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "castExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:170:1: castExpression : ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr;
    public final SJaqlParser.castExpression_return castExpression() throws RecognitionException {
        SJaqlParser.castExpression_return retval = new SJaqlParser.castExpression_return();
        retval.start = input.LT(1);
        int castExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token type=null;
        Token char_literal40=null;
        Token char_literal41=null;
        Token string_literal42=null;
        SJaqlParser.generalPathExpression_return expr = null;


        EvaluationExpression type_tree=null;
        EvaluationExpression char_literal40_tree=null;
        EvaluationExpression char_literal41_tree=null;
        EvaluationExpression string_literal42_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 18) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:171:2: ( ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression ) -> { type != null }? -> $expr)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:171:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:171:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )
            int alt22=3;
            alt22 = dfa22.predict(input);
            switch (alt22) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:171:5: '(' type= ID ')' expr= generalPathExpression
                    {
                    char_literal40=(Token)match(input,30,FOLLOW_30_in_castExpression864); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal40);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression868); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);

                    char_literal41=(Token)match(input,32,FOLLOW_32_in_castExpression870); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal41);

                    pushFollow(FOLLOW_generalPathExpression_in_castExpression874);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:172:4: expr= generalPathExpression 'as' type= ID
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression881);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());
                    string_literal42=(Token)match(input,56,FOLLOW_56_in_castExpression883); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_56.add(string_literal42);

                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression887); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);


                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:173:4: expr= generalPathExpression
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression894);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    }
                    break;

            }



            // AST REWRITE
            // elements: expr
            // token labels: 
            // rule labels: retval, expr
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_expr=new RewriteRuleSubtreeStream(adaptor,"rule expr",expr!=null?expr.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 174:2: -> { type != null }?
            if ( type != null ) {
                adaptor.addChild(root_0,  coerce((type!=null?type.getText():null), (expr!=null?((EvaluationExpression)expr.tree):null)) );

            }
            else // 175:2: -> $expr
            {
                adaptor.addChild(root_0, stream_expr.nextTree());

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
            if ( state.backtracking>0 ) { memoize(input, 18, castExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "castExpression"

    public static class generalPathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "generalPathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:177:1: generalPathExpression : (value= valueExpression path= pathExpression -> | valueExpression );
    public final SJaqlParser.generalPathExpression_return generalPathExpression() throws RecognitionException {
        SJaqlParser.generalPathExpression_return retval = new SJaqlParser.generalPathExpression_return();
        retval.start = input.LT(1);
        int generalPathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.valueExpression_return value = null;

        SJaqlParser.pathExpression_return path = null;

        SJaqlParser.valueExpression_return valueExpression43 = null;


        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 19) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:2: (value= valueExpression path= pathExpression -> | valueExpression )
            int alt23=2;
            alt23 = dfa23.predict(input);
            switch (alt23) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:4: value= valueExpression path= pathExpression
                    {
                    pushFollow(FOLLOW_valueExpression_in_generalPathExpression921);
                    value=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(value.getTree());
                    pushFollow(FOLLOW_pathExpression_in_generalPathExpression925);
                    path=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


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
                    // 178:46: ->
                    {
                        adaptor.addChild(root_0,  PathExpression.wrapIfNecessary((value!=null?((EvaluationExpression)value.tree):null), (path!=null?((EvaluationExpression)path.tree):null)) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:179:4: valueExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_valueExpression_in_generalPathExpression935);
                    valueExpression43=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, valueExpression43.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 19, generalPathExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "generalPathExpression"

    protected static class contextAwarePathExpression_scope {
        List<EvaluationExpression> fragments;
    }
    protected Stack contextAwarePathExpression_stack = new Stack();

    public static class contextAwarePathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "contextAwarePathExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:181:1: contextAwarePathExpression[EvaluationExpression context] : start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) ;
    public final SJaqlParser.contextAwarePathExpression_return contextAwarePathExpression(EvaluationExpression context) throws RecognitionException {
        contextAwarePathExpression_stack.push(new contextAwarePathExpression_scope());
        SJaqlParser.contextAwarePathExpression_return retval = new SJaqlParser.contextAwarePathExpression_return();
        retval.start = input.LT(1);
        int contextAwarePathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token start=null;
        Token field=null;
        Token char_literal44=null;
        SJaqlParser.arrayAccess_return arrayAccess45 = null;


        EvaluationExpression start_tree=null;
        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal44_tree=null;
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
         ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 20) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:3: (start= ID ( ( '.' (field= ID ) ) | arrayAccess )* -> ^( EXPRESSION[\"PathExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:184:5: start= ID ( ( '.' (field= ID ) ) | arrayAccess )*
            {
            start=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression958); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(start);

            if ( state.backtracking==0 ) {
               if(context != null) ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(context); ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((start!=null?start.getText():null)));
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:5: ( ( '.' (field= ID ) ) | arrayAccess )*
            loop24:
            do {
                int alt24=3;
                int LA24_0 = input.LA(1);

                if ( (LA24_0==57) ) {
                    alt24=1;
                }
                else if ( (LA24_0==63) ) {
                    alt24=2;
                }


                switch (alt24) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:7: ( '.' (field= ID ) )
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:7: ( '.' (field= ID ) )
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:8: '.' (field= ID )
            	    {
            	    char_literal44=(Token)match(input,57,FOLLOW_57_in_contextAwarePathExpression969); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_57.add(char_literal44);

            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:12: (field= ID )
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:185:13: field= ID
            	    {
            	    field=(Token)match(input,ID,FOLLOW_ID_in_contextAwarePathExpression974); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(field);

            	    if ( state.backtracking==0 ) {
            	       ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
            	    }

            	    }


            	    }


            	    }
            	    break;
            	case 2 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:186:11: arrayAccess
            	    {
            	    pushFollow(FOLLOW_arrayAccess_in_contextAwarePathExpression992);
            	    arrayAccess45=arrayAccess();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess45.getTree());
            	    if ( state.backtracking==0 ) {
            	       ((contextAwarePathExpression_scope)contextAwarePathExpression_stack.peek()).fragments.add((arrayAccess45!=null?((EvaluationExpression)arrayAccess45.tree):null)); 
            	    }

            	    }
            	    break;

            	default :
            	    break loop24;
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
            // 186:93: -> ^( EXPRESSION[\"PathExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:186:97: ^( EXPRESSION[\"PathExpression\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 20, contextAwarePathExpression_StartIndex); }
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:188:1: pathExpression : ( ( '.' (field= ID ) ) | arrayAccess )+ ->;
    public final SJaqlParser.pathExpression_return pathExpression() throws RecognitionException {
        pathExpression_stack.push(new pathExpression_scope());
        SJaqlParser.pathExpression_return retval = new SJaqlParser.pathExpression_return();
        retval.start = input.LT(1);
        int pathExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token field=null;
        Token char_literal46=null;
        SJaqlParser.arrayAccess_return arrayAccess47 = null;


        EvaluationExpression field_tree=null;
        EvaluationExpression char_literal46_tree=null;
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_arrayAccess=new RewriteRuleSubtreeStream(adaptor,"rule arrayAccess");
         ((pathExpression_scope)pathExpression_stack.peek()).fragments = new ArrayList<EvaluationExpression>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 21) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:191:3: ( ( ( '.' (field= ID ) ) | arrayAccess )+ ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:5: ( ( '.' (field= ID ) ) | arrayAccess )+
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:5: ( ( '.' (field= ID ) ) | arrayAccess )+
            int cnt25=0;
            loop25:
            do {
                int alt25=3;
                int LA25_0 = input.LA(1);

                if ( (LA25_0==57) ) {
                    int LA25_2 = input.LA(2);

                    if ( (synpred37_SJaql()) ) {
                        alt25=1;
                    }


                }
                else if ( (LA25_0==63) ) {
                    int LA25_3 = input.LA(2);

                    if ( (synpred38_SJaql()) ) {
                        alt25=2;
                    }


                }


                switch (alt25) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:7: ( '.' (field= ID ) )
            	    {
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:7: ( '.' (field= ID ) )
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:8: '.' (field= ID )
            	    {
            	    char_literal46=(Token)match(input,57,FOLLOW_57_in_pathExpression1037); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_57.add(char_literal46);

            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:12: (field= ID )
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:13: field= ID
            	    {
            	    field=(Token)match(input,ID,FOLLOW_ID_in_pathExpression1042); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(field);

            	    if ( state.backtracking==0 ) {
            	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add(new ObjectAccess((field!=null?field.getText():null))); 
            	    }

            	    }


            	    }


            	    }
            	    break;
            	case 2 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:193:11: arrayAccess
            	    {
            	    pushFollow(FOLLOW_arrayAccess_in_pathExpression1060);
            	    arrayAccess47=arrayAccess();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_arrayAccess.add(arrayAccess47.getTree());
            	    if ( state.backtracking==0 ) {
            	       ((pathExpression_scope)pathExpression_stack.peek()).fragments.add((arrayAccess47!=null?((EvaluationExpression)arrayAccess47.tree):null)); 
            	    }

            	    }
            	    break;

            	default :
            	    if ( cnt25 >= 1 ) break loop25;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(25, input);
                        throw eee;
                }
                cnt25++;
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
            // 194:3: ->
            {
                adaptor.addChild(root_0,  PathExpression.wrapIfNecessary(((pathExpression_scope)pathExpression_stack.peek()).fragments) );

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
            if ( state.backtracking>0 ) { memoize(input, 21, pathExpression_StartIndex); }
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:196:1: valueExpression : ( methodCall[null] | parenthesesExpression | literal | VAR -> | ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation );
    public final SJaqlParser.valueExpression_return valueExpression() throws RecognitionException {
        SJaqlParser.valueExpression_return retval = new SJaqlParser.valueExpression_return();
        retval.start = input.LT(1);
        int valueExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token VAR51=null;
        Token ID52=null;
        SJaqlParser.methodCall_return methodCall48 = null;

        SJaqlParser.parenthesesExpression_return parenthesesExpression49 = null;

        SJaqlParser.literal_return literal50 = null;

        SJaqlParser.streamIndexAccess_return streamIndexAccess53 = null;

        SJaqlParser.arrayCreation_return arrayCreation54 = null;

        SJaqlParser.objectCreation_return objectCreation55 = null;


        EvaluationExpression VAR51_tree=null;
        EvaluationExpression ID52_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 22) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:197:2: ( methodCall[null] | parenthesesExpression | literal | VAR -> | ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation )
            int alt26=8;
            alt26 = dfa26.predict(input);
            switch (alt26) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:197:4: methodCall[null]
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_methodCall_in_valueExpression1081);
                    methodCall48=methodCall(null);

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, methodCall48.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:198:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression1087);
                    parenthesesExpression49=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression49.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:199:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_literal_in_valueExpression1093);
                    literal50=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal50.getTree());

                    }
                    break;
                case 4 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:4: VAR
                    {
                    VAR51=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression1099); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR51);



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
                    // 200:8: ->
                    {
                        adaptor.addChild(root_0,  makePath(VAR51) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:201:5: ID {...}? =>
                    {
                    ID52=(Token)match(input,ID,FOLLOW_ID_in_valueExpression1109); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID52);

                    if ( !(( hasBinding(ID52, EvaluationExpression.class) )) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "valueExpression", " hasBinding($ID, EvaluationExpression.class) ");
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
                    // 201:59: ->
                    {
                        adaptor.addChild(root_0,  getBinding(ID52, EvaluationExpression.class) );

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 6 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:5: streamIndexAccess
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_streamIndexAccess_in_valueExpression1122);
                    streamIndexAccess53=streamIndexAccess();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, streamIndexAccess53.getTree());

                    }
                    break;
                case 7 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:203:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_arrayCreation_in_valueExpression1127);
                    arrayCreation54=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation54.getTree());

                    }
                    break;
                case 8 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:204:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();

                    pushFollow(FOLLOW_objectCreation_in_valueExpression1133);
                    objectCreation55=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation55.getTree());

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
            if ( state.backtracking>0 ) { memoize(input, 22, valueExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "valueExpression"

    public static class operatorExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:206:1: operatorExpression : op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) ;
    public final SJaqlParser.operatorExpression_return operatorExpression() throws RecognitionException {
        SJaqlParser.operatorExpression_return retval = new SJaqlParser.operatorExpression_return();
        retval.start = input.LT(1);
        int operatorExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        SJaqlParser.operator_return op = null;


        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 23) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:207:2: (op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:207:4: op= operator
            {
            pushFollow(FOLLOW_operator_in_operatorExpression1146);
            op=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(op.getTree());


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
            // 207:16: -> ^( EXPRESSION[\"NestedOperatorExpression\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:207:19: ^( EXPRESSION[\"NestedOperatorExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "NestedOperatorExpression"), root_1);

                adaptor.addChild(root_1,  (op!=null?op.op:null) );

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
            if ( state.backtracking>0 ) { memoize(input, 23, operatorExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "operatorExpression"

    public static class parenthesesExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "parenthesesExpression"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:209:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
    public final SJaqlParser.parenthesesExpression_return parenthesesExpression() throws RecognitionException {
        SJaqlParser.parenthesesExpression_return retval = new SJaqlParser.parenthesesExpression_return();
        retval.start = input.LT(1);
        int parenthesesExpression_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal56=null;
        Token char_literal58=null;
        SJaqlParser.expression_return expression57 = null;


        EvaluationExpression char_literal56_tree=null;
        EvaluationExpression char_literal58_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 24) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:2: ( ( '(' expression ')' ) -> expression )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:4: ( '(' expression ')' )
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:4: ( '(' expression ')' )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:210:5: '(' expression ')'
            {
            char_literal56=(Token)match(input,30,FOLLOW_30_in_parenthesesExpression1167); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal56);

            pushFollow(FOLLOW_expression_in_parenthesesExpression1169);
            expression57=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression57.getTree());
            char_literal58=(Token)match(input,32,FOLLOW_32_in_parenthesesExpression1171); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal58);


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
            // 210:25: -> expression
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
            if ( state.backtracking>0 ) { memoize(input, 24, parenthesesExpression_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "parenthesesExpression"

    public static class methodCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "methodCall"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:212:1: methodCall[EvaluationExpression targetExpr] : name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->;
    public final SJaqlParser.methodCall_return methodCall(EvaluationExpression targetExpr) throws RecognitionException {
        SJaqlParser.methodCall_return retval = new SJaqlParser.methodCall_return();
        retval.start = input.LT(1);
        int methodCall_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal59=null;
        Token char_literal60=null;
        Token char_literal61=null;
        SJaqlParser.expression_return param = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal59_tree=null;
        EvaluationExpression char_literal60_tree=null;
        EvaluationExpression char_literal61_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<EvaluationExpression> params = new ArrayList(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 25) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:214:2: (name= ID '(' (param= expression ( ',' param= expression )* )? ')' ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:214:4: name= ID '(' (param= expression ( ',' param= expression )* )? ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_methodCall1194); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            char_literal59=(Token)match(input,30,FOLLOW_30_in_methodCall1196); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_30.add(char_literal59);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:215:2: (param= expression ( ',' param= expression )* )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( ((LA28_0>=ID && LA28_0<=STRING)||(LA28_0>=DECIMAL && LA28_0<=UINT)||LA28_0==30||(LA28_0>=52 && LA28_0<=55)||LA28_0==58||(LA28_0>=60 && LA28_0<=63)||(LA28_0>=65 && LA28_0<=66)) ) {
                alt28=1;
            }
            switch (alt28) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:215:3: param= expression ( ',' param= expression )*
                    {
                    pushFollow(FOLLOW_expression_in_methodCall1203);
                    param=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(param.getTree());
                    if ( state.backtracking==0 ) {
                       params.add((param!=null?((EvaluationExpression)param.tree):null)); 
                    }
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:216:2: ( ',' param= expression )*
                    loop27:
                    do {
                        int alt27=2;
                        int LA27_0 = input.LA(1);

                        if ( (LA27_0==31) ) {
                            alt27=1;
                        }


                        switch (alt27) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:216:3: ',' param= expression
                    	    {
                    	    char_literal60=(Token)match(input,31,FOLLOW_31_in_methodCall1209); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal60);

                    	    pushFollow(FOLLOW_expression_in_methodCall1213);
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
                    	    break loop27;
                        }
                    } while (true);


                    }
                    break;

            }

            char_literal61=(Token)match(input,32,FOLLOW_32_in_methodCall1223); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_32.add(char_literal61);



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
            // 217:6: ->
            {
                adaptor.addChild(root_0,  createCheckedMethodCall(name, targetExpr, params.toArray(new EvaluationExpression[params.size()])) );

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
            if ( state.backtracking>0 ) { memoize(input, 25, methodCall_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "methodCall"

    public static class fieldAssignment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "fieldAssignment"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:219:1: fieldAssignment : ( ID ':' expression -> | VAR ( '.' ( ID -> | STAR -> | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class)] ':' e= expression ->) | ':' e2= expression -> | '=' op= operator {...}? => | ->) );
    public final SJaqlParser.fieldAssignment_return fieldAssignment() throws RecognitionException {
        SJaqlParser.fieldAssignment_return retval = new SJaqlParser.fieldAssignment_return();
        retval.start = input.LT(1);
        int fieldAssignment_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token ID62=null;
        Token char_literal63=null;
        Token VAR65=null;
        Token char_literal66=null;
        Token ID67=null;
        Token STAR68=null;
        Token char_literal69=null;
        Token char_literal70=null;
        Token char_literal71=null;
        SJaqlParser.contextAwarePathExpression_return p = null;

        SJaqlParser.expression_return e = null;

        SJaqlParser.expression_return e2 = null;

        SJaqlParser.operator_return op = null;

        SJaqlParser.expression_return expression64 = null;


        EvaluationExpression ID62_tree=null;
        EvaluationExpression char_literal63_tree=null;
        EvaluationExpression VAR65_tree=null;
        EvaluationExpression char_literal66_tree=null;
        EvaluationExpression ID67_tree=null;
        EvaluationExpression STAR68_tree=null;
        EvaluationExpression char_literal69_tree=null;
        EvaluationExpression char_literal70_tree=null;
        EvaluationExpression char_literal71_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_28=new RewriteRuleTokenStream(adaptor,"token 28");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_contextAwarePathExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwarePathExpression");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 26) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:220:2: ( ID ':' expression -> | VAR ( '.' ( ID -> | STAR -> | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class)] ':' e= expression ->) | ':' e2= expression -> | '=' op= operator {...}? => | ->) )
            int alt31=2;
            int LA31_0 = input.LA(1);

            if ( (LA31_0==ID) ) {
                alt31=1;
            }
            else if ( (LA31_0==VAR) ) {
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:220:4: ID ':' expression
                    {
                    ID62=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1237); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID62);

                    char_literal63=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1239); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal63);

                    pushFollow(FOLLOW_expression_in_fieldAssignment1241);
                    expression64=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression64.getTree());
                    if ( state.backtracking==0 ) {
                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment((ID62!=null?ID62.getText():null), (expression64!=null?((EvaluationExpression)expression64.tree):null))); 
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
                    // 221:104: ->
                    {
                        root_0 = null;
                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:222:5: VAR ( '.' ( ID -> | STAR -> | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class)] ':' e= expression ->) | ':' e2= expression -> | '=' op= operator {...}? => | ->)
                    {
                    VAR65=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1256); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR65);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:223:5: ( '.' ( ID -> | STAR -> | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class)] ':' e= expression ->) | ':' e2= expression -> | '=' op= operator {...}? => | ->)
                    int alt30=4;
                    switch ( input.LA(1) ) {
                    case 57:
                        {
                        alt30=1;
                        }
                        break;
                    case 35:
                        {
                        alt30=2;
                        }
                        break;
                    case 28:
                        {
                        alt30=3;
                        }
                        break;
                    case EOF:
                    case 31:
                    case 59:
                        {
                        alt30=4;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 30, 0, input);

                        throw nvae;
                    }

                    switch (alt30) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:224:7: '.' ( ID -> | STAR -> | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class)] ':' e= expression ->)
                            {
                            char_literal66=(Token)match(input,57,FOLLOW_57_in_fieldAssignment1272); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_57.add(char_literal66);

                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:224:11: ( ID -> | STAR -> | p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class)] ':' e= expression ->)
                            int alt29=3;
                            int LA29_0 = input.LA(1);

                            if ( (LA29_0==ID) ) {
                                int LA29_1 = input.LA(2);

                                if ( (LA29_1==EOF||LA29_1==31||LA29_1==59) ) {
                                    alt29=1;
                                }
                                else if ( (LA29_1==35||LA29_1==57||LA29_1==63) ) {
                                    alt29=3;
                                }
                                else {
                                    if (state.backtracking>0) {state.failed=true; return retval;}
                                    NoViableAltException nvae =
                                        new NoViableAltException("", 29, 1, input);

                                    throw nvae;
                                }
                            }
                            else if ( (LA29_0==STAR) ) {
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
                                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:224:12: ID
                                    {
                                    ID67=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1275); if (state.failed) return retval; 
                                    if ( state.backtracking==0 ) stream_ID.add(ID67);

                                    if ( state.backtracking==0 ) {
                                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment((ID67!=null?ID67.getText():null), makePath(VAR65, (ID67!=null?ID67.getText():null)))); 
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
                                    // 224:122: ->
                                    {
                                        root_0 = null;
                                    }

                                    retval.tree = root_0;}
                                    }
                                    break;
                                case 2 :
                                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:225:11: STAR
                                    {
                                    STAR68=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment1291); if (state.failed) return retval; 
                                    if ( state.backtracking==0 ) stream_STAR.add(STAR68);

                                    if ( state.backtracking==0 ) {
                                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.CopyFields(makePath(VAR65))); 
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
                                    // 225:98: ->
                                    {
                                        root_0 = null;
                                    }

                                    retval.tree = root_0;}
                                    }
                                    break;
                                case 3 :
                                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:226:11: p= contextAwarePathExpression[getBinding($VAR, JsonStreamExpression.class)] ':' e= expression
                                    {
                                    pushFollow(FOLLOW_contextAwarePathExpression_in_fieldAssignment1309);
                                    p=contextAwarePathExpression(getBinding(VAR65, JsonStreamExpression.class));

                                    state._fsp--;
                                    if (state.failed) return retval;
                                    if ( state.backtracking==0 ) stream_contextAwarePathExpression.add(p.getTree());
                                    char_literal69=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1312); if (state.failed) return retval; 
                                    if ( state.backtracking==0 ) stream_35.add(char_literal69);

                                    pushFollow(FOLLOW_expression_in_fieldAssignment1316);
                                    e=expression();

                                    state._fsp--;
                                    if (state.failed) return retval;
                                    if ( state.backtracking==0 ) stream_expression.add(e.getTree());
                                    if ( state.backtracking==0 ) {
                                       ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.TagMapping((p!=null?((EvaluationExpression)p.tree):null), (e!=null?((EvaluationExpression)e.tree):null))); 
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
                                    // 227:95: ->
                                    {
                                        root_0 = null;
                                    }

                                    retval.tree = root_0;}
                                    }
                                    break;

                            }


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:229:7: ':' e2= expression
                            {
                            char_literal70=(Token)match(input,35,FOLLOW_35_in_fieldAssignment1350); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_35.add(char_literal70);

                            pushFollow(FOLLOW_expression_in_fieldAssignment1354);
                            e2=expression();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_expression.add(e2.getTree());
                            if ( state.backtracking==0 ) {
                               ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.TagMapping(makePath(VAR65), (e2!=null?((EvaluationExpression)e2.tree):null))); 
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
                            // 229:117: ->
                            {
                                root_0 = null;
                            }

                            retval.tree = root_0;}
                            }
                            break;
                        case 3 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:230:7: '=' op= operator {...}? =>
                            {
                            char_literal71=(Token)match(input,28,FOLLOW_28_in_fieldAssignment1366); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_28.add(char_literal71);

                            pushFollow(FOLLOW_operator_in_fieldAssignment1370);
                            op=operator();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_operator.add(op.getTree());
                            if ( !(( setInnerOutput(VAR65, (op!=null?op.op:null)) )) ) {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                throw new FailedPredicateException(input, "fieldAssignment", " setInnerOutput($VAR, $op.op) ");
                            }

                            }
                            break;
                        case 4 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:231:19: 
                            {
                            if ( state.backtracking==0 ) {
                               ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment((VAR65!=null?VAR65.getText():null).substring(1), makePath(VAR65))); 
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
                            // 231:130: ->
                            {
                                root_0 = null;
                            }

                            retval.tree = root_0;}
                            }
                            break;

                    }


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
            if ( state.backtracking>0 ) { memoize(input, 26, fieldAssignment_StartIndex); }
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:234:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
    public final SJaqlParser.objectCreation_return objectCreation() throws RecognitionException {
        objectCreation_stack.push(new objectCreation_scope());
        SJaqlParser.objectCreation_return retval = new SJaqlParser.objectCreation_return();
        retval.start = input.LT(1);
        int objectCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal72=null;
        Token char_literal74=null;
        Token char_literal76=null;
        Token char_literal77=null;
        SJaqlParser.fieldAssignment_return fieldAssignment73 = null;

        SJaqlParser.fieldAssignment_return fieldAssignment75 = null;


        EvaluationExpression char_literal72_tree=null;
        EvaluationExpression char_literal74_tree=null;
        EvaluationExpression char_literal76_tree=null;
        EvaluationExpression char_literal77_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 27) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal72=(Token)match(input,58,FOLLOW_58_in_objectCreation1413); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_58.add(char_literal72);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( ((LA34_0>=ID && LA34_0<=VAR)) ) {
                alt34=1;
            }
            switch (alt34) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1416);
                    fieldAssignment73=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment73.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:25: ( ',' fieldAssignment )*
                    loop32:
                    do {
                        int alt32=2;
                        int LA32_0 = input.LA(1);

                        if ( (LA32_0==31) ) {
                            int LA32_1 = input.LA(2);

                            if ( ((LA32_1>=ID && LA32_1<=VAR)) ) {
                                alt32=1;
                            }


                        }


                        switch (alt32) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:26: ',' fieldAssignment
                    	    {
                    	    char_literal74=(Token)match(input,31,FOLLOW_31_in_objectCreation1419); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal74);

                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation1421);
                    	    fieldAssignment75=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment75.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop32;
                        }
                    } while (true);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:48: ( ',' )?
                    int alt33=2;
                    int LA33_0 = input.LA(1);

                    if ( (LA33_0==31) ) {
                        alt33=1;
                    }
                    switch (alt33) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                            {
                            char_literal76=(Token)match(input,31,FOLLOW_31_in_objectCreation1425); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_31.add(char_literal76);


                            }
                            break;

                    }


                    }
                    break;

            }

            char_literal77=(Token)match(input,59,FOLLOW_59_in_objectCreation1430); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(char_literal77);



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
            // 237:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:237:62: ^( EXPRESSION[\"ObjectCreation\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 27, objectCreation_StartIndex); }
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:243:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->);
    public final SJaqlParser.literal_return literal() throws RecognitionException {
        SJaqlParser.literal_return retval = new SJaqlParser.literal_return();
        retval.start = input.LT(1);
        int literal_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token val=null;
        Token string_literal78=null;

        EvaluationExpression val_tree=null;
        EvaluationExpression string_literal78_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_DECIMAL=new RewriteRuleTokenStream(adaptor,"token DECIMAL");
        RewriteRuleTokenStream stream_62=new RewriteRuleTokenStream(adaptor,"token 62");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 28) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:244:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= INTEGER -> ^( EXPRESSION[\"ConstantExpression\"] ) | val= UINT -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->)
            int alt35=7;
            switch ( input.LA(1) ) {
            case 60:
                {
                alt35=1;
                }
                break;
            case 61:
                {
                alt35=2;
                }
                break;
            case DECIMAL:
                {
                alt35=3;
                }
                break;
            case STRING:
                {
                alt35=4;
                }
                break;
            case INTEGER:
                {
                alt35=5;
                }
                break;
            case UINT:
                {
                alt35=6;
                }
                break;
            case 62:
                {
                alt35=7;
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:244:4: val= 'true'
                    {
                    val=(Token)match(input,60,FOLLOW_60_in_literal1454); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_60.add(val);



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
                    // 244:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:244:18: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:245:4: val= 'false'
                    {
                    val=(Token)match(input,61,FOLLOW_61_in_literal1470); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_61.add(val);



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
                    // 245:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:245:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal1486); if (state.failed) return retval; 
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
                    // 246:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:246:19: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal1502); if (state.failed) return retval; 
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
                    // 247:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:247:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression"), root_1);

                        adaptor.addChild(root_1,  val.getText() );

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 5 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:248:5: val= INTEGER
                    {
                    val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal1519); if (state.failed) return retval; 
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
                    // 248:17: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:248:20: ^( EXPRESSION[\"ConstantExpression\"] )
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:249:5: val= UINT
                    {
                    val=(Token)match(input,UINT,FOLLOW_UINT_in_literal1536); if (state.failed) return retval; 
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
                    // 249:14: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:249:17: ^( EXPRESSION[\"ConstantExpression\"] )
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
                case 7 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:250:5: 'null'
                    {
                    string_literal78=(Token)match(input,62,FOLLOW_62_in_literal1551); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_62.add(string_literal78);



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
                    // 250:12: ->
                    {
                        adaptor.addChild(root_0,  EvaluationExpression.NULL );

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
            if ( state.backtracking>0 ) { memoize(input, 28, literal_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "literal"

    public static class arrayAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayAccess"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:252:1: arrayAccess : ( '[' STAR ']' path= pathExpression -> ^( EXPRESSION[\"ArrayProjection\"] $path) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
    public final SJaqlParser.arrayAccess_return arrayAccess() throws RecognitionException {
        SJaqlParser.arrayAccess_return retval = new SJaqlParser.arrayAccess_return();
        retval.start = input.LT(1);
        int arrayAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token pos=null;
        Token start=null;
        Token end=null;
        Token char_literal79=null;
        Token STAR80=null;
        Token char_literal81=null;
        Token char_literal82=null;
        Token char_literal83=null;
        Token char_literal84=null;
        Token char_literal85=null;
        Token char_literal86=null;
        SJaqlParser.pathExpression_return path = null;


        EvaluationExpression pos_tree=null;
        EvaluationExpression start_tree=null;
        EvaluationExpression end_tree=null;
        EvaluationExpression char_literal79_tree=null;
        EvaluationExpression STAR80_tree=null;
        EvaluationExpression char_literal81_tree=null;
        EvaluationExpression char_literal82_tree=null;
        EvaluationExpression char_literal83_tree=null;
        EvaluationExpression char_literal84_tree=null;
        EvaluationExpression char_literal85_tree=null;
        EvaluationExpression char_literal86_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 29) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:3: ( '[' STAR ']' path= pathExpression -> ^( EXPRESSION[\"ArrayProjection\"] $path) | '[' (pos= INTEGER | pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt39=3;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==63) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt39=1;
                    }
                    break;
                case INTEGER:
                    {
                    int LA39_3 = input.LA(3);

                    if ( (LA39_3==64) ) {
                        alt39=2;
                    }
                    else if ( (LA39_3==35) ) {
                        alt39=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 39, 3, input);

                        throw nvae;
                    }
                    }
                    break;
                case UINT:
                    {
                    int LA39_4 = input.LA(3);

                    if ( (LA39_4==64) ) {
                        alt39=2;
                    }
                    else if ( (LA39_4==35) ) {
                        alt39=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 39, 4, input);

                        throw nvae;
                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 39, 1, input);

                    throw nvae;
                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 39, 0, input);

                throw nvae;
            }
            switch (alt39) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:253:5: '[' STAR ']' path= pathExpression
                    {
                    char_literal79=(Token)match(input,63,FOLLOW_63_in_arrayAccess1565); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal79);

                    STAR80=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1567); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR80);

                    char_literal81=(Token)match(input,64,FOLLOW_64_in_arrayAccess1569); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal81);

                    pushFollow(FOLLOW_pathExpression_in_arrayAccess1573);
                    path=pathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());


                    // AST REWRITE
                    // elements: path
                    // token labels: 
                    // rule labels: retval, path
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 254:3: -> ^( EXPRESSION[\"ArrayProjection\"] $path)
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:254:6: ^( EXPRESSION[\"ArrayProjection\"] $path)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayProjection"), root_1);

                        adaptor.addChild(root_1, stream_path.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }

                    retval.tree = root_0;}
                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:5: '[' (pos= INTEGER | pos= UINT ) ']'
                    {
                    char_literal82=(Token)match(input,63,FOLLOW_63_in_arrayAccess1593); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal82);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:9: (pos= INTEGER | pos= UINT )
                    int alt36=2;
                    int LA36_0 = input.LA(1);

                    if ( (LA36_0==INTEGER) ) {
                        alt36=1;
                    }
                    else if ( (LA36_0==UINT) ) {
                        alt36=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 36, 0, input);

                        throw nvae;
                    }
                    switch (alt36) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:10: pos= INTEGER
                            {
                            pos=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1598); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(pos);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:255:24: pos= UINT
                            {
                            pos=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1604); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(pos);


                            }
                            break;

                    }

                    char_literal83=(Token)match(input,64,FOLLOW_64_in_arrayAccess1607); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal83);



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
                    // 256:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:256:6: ^( EXPRESSION[\"ArrayAccess\"] )
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
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:257:5: '[' (start= INTEGER | start= UINT ) ':' (end= INTEGER | end= UINT ) ']'
                    {
                    char_literal84=(Token)match(input,63,FOLLOW_63_in_arrayAccess1625); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_63.add(char_literal84);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:257:9: (start= INTEGER | start= UINT )
                    int alt37=2;
                    int LA37_0 = input.LA(1);

                    if ( (LA37_0==INTEGER) ) {
                        alt37=1;
                    }
                    else if ( (LA37_0==UINT) ) {
                        alt37=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 37, 0, input);

                        throw nvae;
                    }
                    switch (alt37) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:257:10: start= INTEGER
                            {
                            start=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1630); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(start);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:257:26: start= UINT
                            {
                            start=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1636); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(start);


                            }
                            break;

                    }

                    char_literal85=(Token)match(input,35,FOLLOW_35_in_arrayAccess1639); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_35.add(char_literal85);

                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:257:42: (end= INTEGER | end= UINT )
                    int alt38=2;
                    int LA38_0 = input.LA(1);

                    if ( (LA38_0==INTEGER) ) {
                        alt38=1;
                    }
                    else if ( (LA38_0==UINT) ) {
                        alt38=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 38, 0, input);

                        throw nvae;
                    }
                    switch (alt38) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:257:43: end= INTEGER
                            {
                            end=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1644); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(end);


                            }
                            break;
                        case 2 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:257:57: end= UINT
                            {
                            end=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1650); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(end);


                            }
                            break;

                    }

                    char_literal86=(Token)match(input,64,FOLLOW_64_in_arrayAccess1653); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(char_literal86);



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
                    // 258:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:258:6: ^( EXPRESSION[\"ArrayAccess\"] )
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
            if ( state.backtracking>0 ) { memoize(input, 29, arrayAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"

    public static class streamIndexAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "streamIndexAccess"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:260:1: streamIndexAccess : op= VAR {...}? => '[' path= generalPathExpression ']' {...}? ->;
    public final SJaqlParser.streamIndexAccess_return streamIndexAccess() throws RecognitionException {
        SJaqlParser.streamIndexAccess_return retval = new SJaqlParser.streamIndexAccess_return();
        retval.start = input.LT(1);
        int streamIndexAccess_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token op=null;
        Token char_literal87=null;
        Token char_literal88=null;
        SJaqlParser.generalPathExpression_return path = null;


        EvaluationExpression op_tree=null;
        EvaluationExpression char_literal87_tree=null;
        EvaluationExpression char_literal88_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 30) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:261:3: (op= VAR {...}? => '[' path= generalPathExpression ']' {...}? ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:261:5: op= VAR {...}? => '[' path= generalPathExpression ']' {...}?
            {
            op=(Token)match(input,VAR,FOLLOW_VAR_in_streamIndexAccess1681); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(op);

            if ( !(( getRawBinding(op, JsonStreamExpression.class) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "streamIndexAccess", " getRawBinding($op, JsonStreamExpression.class) != null ");
            }
            char_literal87=(Token)match(input,63,FOLLOW_63_in_streamIndexAccess1690); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal87);

            pushFollow(FOLLOW_generalPathExpression_in_streamIndexAccess1694);
            path=generalPathExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_generalPathExpression.add(path.getTree());
            char_literal88=(Token)match(input,64,FOLLOW_64_in_streamIndexAccess1696); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal88);

            if ( !(( !((path!=null?((EvaluationExpression)path.tree):null) instanceof ConstantExpression) )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "streamIndexAccess", " !($path.tree instanceof ConstantExpression) ");
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
            // 263:3: ->
            {
                adaptor.addChild(root_0,  new StreamIndexExpression(getBinding(op, JsonStreamExpression.class).getStream(), (path!=null?((EvaluationExpression)path.tree):null)) );

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
            if ( state.backtracking>0 ) { memoize(input, 30, streamIndexAccess_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "streamIndexAccess"

    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayCreation"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:265:1: arrayCreation : '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) ;
    public final SJaqlParser.arrayCreation_return arrayCreation() throws RecognitionException {
        SJaqlParser.arrayCreation_return retval = new SJaqlParser.arrayCreation_return();
        retval.start = input.LT(1);
        int arrayCreation_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token char_literal89=null;
        Token char_literal90=null;
        Token char_literal91=null;
        Token char_literal92=null;
        List list_elems=null;
        RuleReturnScope elems = null;
        EvaluationExpression char_literal89_tree=null;
        EvaluationExpression char_literal90_tree=null;
        EvaluationExpression char_literal91_tree=null;
        EvaluationExpression char_literal92_tree=null;
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 31) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:2: ( '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:5: '[' elems+= expression ( ',' elems+= expression )* ( ',' )? ']'
            {
            char_literal89=(Token)match(input,63,FOLLOW_63_in_arrayCreation1715); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal89);

            pushFollow(FOLLOW_expression_in_arrayCreation1719);
            elems=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            if (list_elems==null) list_elems=new ArrayList();
            list_elems.add(elems.getTree());

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:27: ( ',' elems+= expression )*
            loop40:
            do {
                int alt40=2;
                int LA40_0 = input.LA(1);

                if ( (LA40_0==31) ) {
                    int LA40_1 = input.LA(2);

                    if ( ((LA40_1>=ID && LA40_1<=STRING)||(LA40_1>=DECIMAL && LA40_1<=UINT)||LA40_1==30||(LA40_1>=52 && LA40_1<=55)||LA40_1==58||(LA40_1>=60 && LA40_1<=63)||(LA40_1>=65 && LA40_1<=66)) ) {
                        alt40=1;
                    }


                }


                switch (alt40) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:28: ',' elems+= expression
            	    {
            	    char_literal90=(Token)match(input,31,FOLLOW_31_in_arrayCreation1722); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_31.add(char_literal90);

            	    pushFollow(FOLLOW_expression_in_arrayCreation1726);
            	    elems=expression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
            	    if (list_elems==null) list_elems=new ArrayList();
            	    list_elems.add(elems.getTree());


            	    }
            	    break;

            	default :
            	    break loop40;
                }
            } while (true);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:52: ( ',' )?
            int alt41=2;
            int LA41_0 = input.LA(1);

            if ( (LA41_0==31) ) {
                alt41=1;
            }
            switch (alt41) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ','
                    {
                    char_literal91=(Token)match(input,31,FOLLOW_31_in_arrayCreation1730); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal91);


                    }
                    break;

            }

            char_literal92=(Token)match(input,64,FOLLOW_64_in_arrayCreation1733); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal92);



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
            // 266:61: -> ^( EXPRESSION[\"ArrayCreation\"] )
            {
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:266:64: ^( EXPRESSION[\"ArrayCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot((EvaluationExpression)adaptor.create(EXPRESSION, "ArrayCreation"), root_1);

                adaptor.addChild(root_1,  list_elems.toArray(new EvaluationExpression[list_elems.size()]) );

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
            if ( state.backtracking>0 ) { memoize(input, 31, arrayCreation_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayCreation"

    protected static class operator_scope {
        Operator<?> result;
        int numInputs;
        Map<JsonStream, List<ExpressionTag>> inputTags;
    }
    protected Stack operator_stack = new Stack();

    public static class operator_return extends ParserRuleReturnScope {
        public Operator<?> op=null;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operator"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:268:1: operator returns [Operator<?> op=null] : opRule= ( readOperator | writeOperator | genericOperator ) ;
    public final SJaqlParser.operator_return operator() throws RecognitionException {
        operator_stack.push(new operator_scope());
        SJaqlParser.operator_return retval = new SJaqlParser.operator_return();
        retval.start = input.LT(1);
        int operator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token opRule=null;
        SJaqlParser.readOperator_return readOperator93 = null;

        SJaqlParser.writeOperator_return writeOperator94 = null;

        SJaqlParser.genericOperator_return genericOperator95 = null;


        EvaluationExpression opRule_tree=null;


          if(state.backtracking == 0) 
        	  getContext().getBindings().addScope();
        	((operator_scope)operator_stack.peek()).inputTags = new IdentityHashMap<JsonStream, List<ExpressionTag>>();

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 32) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:2: (opRule= ( readOperator | writeOperator | genericOperator ) )
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:4: opRule= ( readOperator | writeOperator | genericOperator )
            {
            root_0 = (EvaluationExpression)adaptor.nil();

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:11: ( readOperator | writeOperator | genericOperator )
            int alt42=3;
            switch ( input.LA(1) ) {
            case 65:
                {
                alt42=1;
                }
                break;
            case 66:
                {
                alt42=2;
                }
                break;
            case ID:
                {
                alt42=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 42, 0, input);

                throw nvae;
            }

            switch (alt42) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:12: readOperator
                    {
                    pushFollow(FOLLOW_readOperator_in_operator1770);
                    readOperator93=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator93.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:27: writeOperator
                    {
                    pushFollow(FOLLOW_writeOperator_in_operator1774);
                    writeOperator94=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator94.getTree());

                    }
                    break;
                case 3 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:281:43: genericOperator
                    {
                    pushFollow(FOLLOW_genericOperator_in_operator1778);
                    genericOperator95=genericOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, genericOperator95.getTree());

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
            if ( state.backtracking==0 ) {

                getContext().getBindings().removeScope();

            }
        }
             finally {
            if ( state.backtracking>0 ) { memoize(input, 32, operator_StartIndex); }
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
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:286:1: readOperator : 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.readOperator_return readOperator() throws RecognitionException {
        SJaqlParser.readOperator_return retval = new SJaqlParser.readOperator_return();
        retval.start = input.LT(1);
        int readOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token loc=null;
        Token file=null;
        Token string_literal96=null;
        Token char_literal97=null;
        Token char_literal98=null;

        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal96_tree=null;
        EvaluationExpression char_literal97_tree=null;
        EvaluationExpression char_literal98_tree=null;
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_65=new RewriteRuleTokenStream(adaptor,"token 65");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 33) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:2: ( 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:4: 'read' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal96=(Token)match(input,65,FOLLOW_65_in_readOperator1792); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_65.add(string_literal96);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:11: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt44=2;
            int LA44_0 = input.LA(1);

            if ( (LA44_0==ID) ) {
                int LA44_1 = input.LA(2);

                if ( (LA44_1==30) ) {
                    alt44=2;
                }
                else if ( (LA44_1==STRING) ) {
                    alt44=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 44, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA44_0==STRING) ) {
                alt44=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 44, 0, input);

                throw nvae;
            }
            switch (alt44) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:12: (loc= ID )? file= STRING
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:15: (loc= ID )?
                    int alt43=2;
                    int LA43_0 = input.LA(1);

                    if ( (LA43_0==ID) ) {
                        alt43=1;
                    }
                    switch (alt43) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1797); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1802); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:287:34: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_readOperator1808); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal97=(Token)match(input,30,FOLLOW_30_in_readOperator1810); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal97);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_readOperator1814); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal98=(Token)match(input,32,FOLLOW_32_in_readOperator1816); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal98);


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
            // 287:133: ->
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
            if ( state.backtracking>0 ) { memoize(input, 33, readOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "readOperator"

    public static class writeOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "writeOperator"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:289:1: writeOperator : 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->;
    public final SJaqlParser.writeOperator_return writeOperator() throws RecognitionException {
        SJaqlParser.writeOperator_return retval = new SJaqlParser.writeOperator_return();
        retval.start = input.LT(1);
        int writeOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token loc=null;
        Token file=null;
        Token string_literal99=null;
        Token string_literal100=null;
        Token char_literal101=null;
        Token char_literal102=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression loc_tree=null;
        EvaluationExpression file_tree=null;
        EvaluationExpression string_literal99_tree=null;
        EvaluationExpression string_literal100_tree=null;
        EvaluationExpression char_literal101_tree=null;
        EvaluationExpression char_literal102_tree=null;
        RewriteRuleTokenStream stream_67=new RewriteRuleTokenStream(adaptor,"token 67");
        RewriteRuleTokenStream stream_66=new RewriteRuleTokenStream(adaptor,"token 66");
        RewriteRuleTokenStream stream_30=new RewriteRuleTokenStream(adaptor,"token 30");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_32=new RewriteRuleTokenStream(adaptor,"token 32");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 34) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:2: ( 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' ) ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:4: 'write' from= VAR 'to' ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            {
            string_literal99=(Token)match(input,66,FOLLOW_66_in_writeOperator1830); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_66.add(string_literal99);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator1834); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            string_literal100=(Token)match(input,67,FOLLOW_67_in_writeOperator1836); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_67.add(string_literal100);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:26: ( (loc= ID )? file= STRING | loc= ID '(' file= STRING ')' )
            int alt46=2;
            int LA46_0 = input.LA(1);

            if ( (LA46_0==ID) ) {
                int LA46_1 = input.LA(2);

                if ( (LA46_1==30) ) {
                    alt46=2;
                }
                else if ( (LA46_1==STRING) ) {
                    alt46=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 46, 1, input);

                    throw nvae;
                }
            }
            else if ( (LA46_0==STRING) ) {
                alt46=1;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 46, 0, input);

                throw nvae;
            }
            switch (alt46) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:27: (loc= ID )? file= STRING
                    {
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:30: (loc= ID )?
                    int alt45=2;
                    int LA45_0 = input.LA(1);

                    if ( (LA45_0==ID) ) {
                        alt45=1;
                    }
                    switch (alt45) {
                        case 1 :
                            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: loc= ID
                            {
                            loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1841); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(loc);


                            }
                            break;

                    }

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1846); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);


                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:290:49: loc= ID '(' file= STRING ')'
                    {
                    loc=(Token)match(input,ID,FOLLOW_ID_in_writeOperator1852); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(loc);

                    char_literal101=(Token)match(input,30,FOLLOW_30_in_writeOperator1854); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_30.add(char_literal101);

                    file=(Token)match(input,STRING,FOLLOW_STRING_in_writeOperator1858); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(file);

                    char_literal102=(Token)match(input,32,FOLLOW_32_in_writeOperator1860); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_32.add(char_literal102);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               
              	Sink sink = new Sink(JsonOutputFormat.class, (file!=null?file.getText():null));
                ((operator_scope)operator_stack.peek()).result = sink;
                sink.setInputs(getBinding(from, JsonStreamExpression.class).getStream());
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
            // 296:3: ->
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
            if ( state.backtracking>0 ) { memoize(input, 34, writeOperator_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "writeOperator"

    protected static class genericOperator_scope {
        OperatorInfo<?> operatorInfo;
    }
    protected Stack genericOperator_stack = new Stack();

    public static class genericOperator_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "genericOperator"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:298:1: genericOperator : name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )* ->;
    public final SJaqlParser.genericOperator_return genericOperator() throws RecognitionException {
        genericOperator_stack.push(new genericOperator_scope());
        SJaqlParser.genericOperator_return retval = new SJaqlParser.genericOperator_return();
        retval.start = input.LT(1);
        int genericOperator_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal106=null;
        SJaqlParser.operatorFlag_return operatorFlag103 = null;

        SJaqlParser.arrayInput_return arrayInput104 = null;

        SJaqlParser.input_return input105 = null;

        SJaqlParser.input_return input107 = null;

        SJaqlParser.operatorOption_return operatorOption108 = null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal106_tree=null;
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_operatorOption=new RewriteRuleSubtreeStream(adaptor,"rule operatorOption");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        RewriteRuleSubtreeStream stream_operatorFlag=new RewriteRuleSubtreeStream(adaptor,"rule operatorFlag");
        RewriteRuleSubtreeStream stream_arrayInput=new RewriteRuleSubtreeStream(adaptor,"rule arrayInput");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 35) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:301:3: (name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )* ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:301:5: name= ID {...}? => ( operatorFlag )* ( arrayInput | input ( ',' input )* ) ( operatorOption )*
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator1880); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((genericOperator_scope)genericOperator_stack.peek()).operatorInfo = findOperatorGreedily(name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "genericOperator", " ($genericOperator::operatorInfo = findOperatorGreedily($name)) != null ");
            }
            if ( state.backtracking==0 ) {
               ((operator_scope)operator_stack.peek()).result = ((genericOperator_scope)genericOperator_stack.peek()).operatorInfo.newInstance(); 
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:303:1: ( operatorFlag )*
            loop47:
            do {
                int alt47=2;
                int LA47_0 = input.LA(1);

                if ( (LA47_0==ID) ) {
                    alt47=1;
                }


                switch (alt47) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: operatorFlag
            	    {
            	    pushFollow(FOLLOW_operatorFlag_in_genericOperator1888);
            	    operatorFlag103=operatorFlag();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorFlag.add(operatorFlag103.getTree());

            	    }
            	    break;

            	default :
            	    break loop47;
                }
            } while (true);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:1: ( arrayInput | input ( ',' input )* )
            int alt49=2;
            int LA49_0 = input.LA(1);

            if ( (LA49_0==63) ) {
                alt49=1;
            }
            else if ( (LA49_0==VAR||LA49_0==68) ) {
                alt49=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 49, 0, input);

                throw nvae;
            }
            switch (alt49) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:2: arrayInput
                    {
                    pushFollow(FOLLOW_arrayInput_in_genericOperator1892);
                    arrayInput104=arrayInput();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arrayInput.add(arrayInput104.getTree());

                    }
                    break;
                case 2 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:15: input ( ',' input )*
                    {
                    pushFollow(FOLLOW_input_in_genericOperator1896);
                    input105=input();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_input.add(input105.getTree());
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:21: ( ',' input )*
                    loop48:
                    do {
                        int alt48=2;
                        int LA48_0 = input.LA(1);

                        if ( (LA48_0==31) ) {
                            int LA48_2 = input.LA(2);

                            if ( (synpred78_SJaql()) ) {
                                alt48=1;
                            }


                        }


                        switch (alt48) {
                    	case 1 :
                    	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:22: ',' input
                    	    {
                    	    char_literal106=(Token)match(input,31,FOLLOW_31_in_genericOperator1899); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_31.add(char_literal106);

                    	    pushFollow(FOLLOW_input_in_genericOperator1901);
                    	    input107=input();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_input.add(input107.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop48;
                        }
                    } while (true);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               if(state.backtracking == 0) 
                  getContext().getBindings().set("$", new JsonStreamExpression(((operator_scope)operator_stack.peek()).result).withTag(JsonStreamExpression.THIS_CONTEXT)); 
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:307:1: ( operatorOption )*
            loop50:
            do {
                int alt50=2;
                int LA50_0 = input.LA(1);

                if ( (LA50_0==ID) ) {
                    int LA50_2 = input.LA(2);

                    if ( (synpred79_SJaql()) ) {
                        alt50=1;
                    }


                }


                switch (alt50) {
            	case 1 :
            	    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: operatorOption
            	    {
            	    pushFollow(FOLLOW_operatorOption_in_genericOperator1909);
            	    operatorOption108=operatorOption();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_operatorOption.add(operatorOption108.getTree());

            	    }
            	    break;

            	default :
            	    break loop50;
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
            // 307:17: ->
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
            if ( state.backtracking>0 ) { memoize(input, 35, genericOperator_StartIndex); }
            genericOperator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "genericOperator"

    protected static class operatorOption_scope {
        OperatorInfo.OperatorPropertyInfo property;
    }
    protected Stack operatorOption_stack = new Stack();

    public static class operatorOption_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorOption"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:309:1: operatorOption : name= ID {...}?expr= contextAwareExpression[null] ->;
    public final SJaqlParser.operatorOption_return operatorOption() throws RecognitionException {
        operatorOption_stack.push(new operatorOption_scope());
        SJaqlParser.operatorOption_return retval = new SJaqlParser.operatorOption_return();
        retval.start = input.LT(1);
        int operatorOption_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 36) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:313:2: (name= ID {...}?expr= contextAwareExpression[null] ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:313:4: name= ID {...}?expr= contextAwareExpression[null]
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorOption1929); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((operatorOption_scope)operatorOption_stack.peek()).property = findOperatorPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "operatorOption", " ($operatorOption::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null ");
            }
            pushFollow(FOLLOW_contextAwareExpression_in_operatorOption1935);
            expr=contextAwareExpression(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
            if ( state.backtracking==0 ) {
               ((operatorOption_scope)operatorOption_stack.peek()).property.setValue(((operator_scope)operator_stack.peek()).result, (expr!=null?((EvaluationExpression)expr.tree):null)); 
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
            // 314:106: ->
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
            if ( state.backtracking>0 ) { memoize(input, 36, operatorOption_StartIndex); }
            operatorOption_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorOption"

    protected static class operatorFlag_scope {
        OperatorInfo.OperatorPropertyInfo property;
    }
    protected Stack operatorFlag_stack = new Stack();

    public static class operatorFlag_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "operatorFlag"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:316:1: operatorFlag : name= ID {...}? ->;
    public final SJaqlParser.operatorFlag_return operatorFlag() throws RecognitionException {
        operatorFlag_stack.push(new operatorFlag_scope());
        SJaqlParser.operatorFlag_return retval = new SJaqlParser.operatorFlag_return();
        retval.start = input.LT(1);
        int operatorFlag_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token name=null;

        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 37) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:3: (name= ID {...}? ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:320:5: name= ID {...}?
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_operatorFlag1956); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);

            if ( !(( (((operatorFlag_scope)operatorFlag_stack.peek()).property = findOperatorPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "operatorFlag", " ($operatorFlag::property = findOperatorPropertyRelunctantly($genericOperator::operatorInfo, $name)) != null ");
            }
            if ( state.backtracking==0 ) {
               if(!((operatorFlag_scope)operatorFlag_stack.peek()).property.isFlag())
                  throw new SimpleException(String.format("Property %s is not a flag", (name!=null?name.getText():null)), name);
                ((operatorFlag_scope)operatorFlag_stack.peek()).property.setValue(((operator_scope)operator_stack.peek()).result, true); 
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
            // 323:64: ->
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
            if ( state.backtracking>0 ) { memoize(input, 37, operatorFlag_StartIndex); }
            operatorFlag_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operatorFlag"

    protected static class input_scope {
        OperatorInfo.InputPropertyInfo inputProperty;
    }
    protected Stack input_stack = new Stack();

    public static class input_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "input"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:325:1: input : (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )? ->;
    public final SJaqlParser.input_return input() throws RecognitionException {
        input_stack.push(new input_scope());
        SJaqlParser.input_return retval = new SJaqlParser.input_return();
        retval.start = input.LT(1);
        int input_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token preserveFlag=null;
        Token name=null;
        Token from=null;
        Token inputOption=null;
        Token string_literal109=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        EvaluationExpression preserveFlag_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression inputOption_tree=null;
        EvaluationExpression string_literal109_tree=null;
        RewriteRuleTokenStream stream_68=new RewriteRuleTokenStream(adaptor,"token 68");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_contextAwareExpression=new RewriteRuleSubtreeStream(adaptor,"rule contextAwareExpression");
        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 38) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:3: ( (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )? ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:5: (preserveFlag= 'preserve' )? (name= VAR 'in' )? from= VAR (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?
            {
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:17: (preserveFlag= 'preserve' )?
            int alt51=2;
            int LA51_0 = input.LA(1);

            if ( (LA51_0==68) ) {
                alt51=1;
            }
            switch (alt51) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: preserveFlag= 'preserve'
                    {
                    preserveFlag=(Token)match(input,68,FOLLOW_68_in_input1978); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_68.add(preserveFlag);


                    }
                    break;

            }

            if ( state.backtracking==0 ) {
            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:33: (name= VAR 'in' )?
            int alt52=2;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==VAR) ) {
                int LA52_1 = input.LA(2);

                if ( (LA52_1==42) ) {
                    alt52=1;
                }
            }
            switch (alt52) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:328:34: name= VAR 'in'
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input1986); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);

                    string_literal109=(Token)match(input,42,FOLLOW_42_in_input1988); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(string_literal109);


                    }
                    break;

            }

            from=(Token)match(input,VAR,FOLLOW_VAR_in_input1994); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                int inputIndex = ((operator_scope)operator_stack.peek()).numInputs++;
                JsonStreamExpression input = getBinding(from, JsonStreamExpression.class);
                ((operator_scope)operator_stack.peek()).result.setInput(inputIndex, input.getStream());
                
                if(preserveFlag != null)
                  setBinding(name != null ? name : from, new JsonStreamExpression(input.getStream(), inputIndex).withTag(ExpressionTag.RETAIN));
                else setBinding(name != null ? name : from, new JsonStreamExpression(input.getStream(), inputIndex));
              //    ((operator_scope)operator_stack.peek()).inputTags.put(input, Arrays.asList(ExpressionTag.RETAIN));

            }
            if ( state.backtracking==0 ) {
               if(state.backtracking == 0) {
                  addScope();
                  getContext().getBindings().set("$", new JsonStreamExpression(((operator_scope)operator_stack.peek()).result).withTag(JsonStreamExpression.THIS_CONTEXT)); 
                }

            }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:344:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?
            int alt53=2;
            alt53 = dfa53.predict(input);
            switch (alt53) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:344:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)]
                    {
                    inputOption=(Token)match(input,ID,FOLLOW_ID_in_input2004); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(inputOption);

                    if ( !(( (((input_scope)input_stack.peek()).inputProperty = findInputPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, inputOption)) != null )) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "input", " ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, $inputOption)) != null ");
                    }
                    pushFollow(FOLLOW_contextAwareExpression_in_input2012);
                    expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).numInputs - 1));

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_contextAwareExpression.add(expr.getTree());
                    if ( state.backtracking==0 ) {
                       ((input_scope)input_stack.peek()).inputProperty.setValue(((operator_scope)operator_stack.peek()).result, ((operator_scope)operator_stack.peek()).numInputs-1, (expr!=null?((EvaluationExpression)expr.tree):null)); 
                    }

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               if(state.backtracking == 0) 
                  removeScope();

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
            // 349:1: ->
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
            if ( state.backtracking>0 ) { memoize(input, 38, input_StartIndex); }
            input_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "input"

    public static class arrayInput_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };

    // $ANTLR start "arrayInput"
    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:351:1: arrayInput : '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR ->;
    public final SJaqlParser.arrayInput_return arrayInput() throws RecognitionException {
        SJaqlParser.arrayInput_return retval = new SJaqlParser.arrayInput_return();
        retval.start = input.LT(1);
        int arrayInput_StartIndex = input.index();
        EvaluationExpression root_0 = null;

        Token from=null;
        Token char_literal110=null;
        Token char_literal111=null;
        Token char_literal112=null;
        Token string_literal113=null;
        Token names=null;
        List list_names=null;

        EvaluationExpression from_tree=null;
        EvaluationExpression char_literal110_tree=null;
        EvaluationExpression char_literal111_tree=null;
        EvaluationExpression char_literal112_tree=null;
        EvaluationExpression string_literal113_tree=null;
        EvaluationExpression names_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_31=new RewriteRuleTokenStream(adaptor,"token 31");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");

        try {
            if ( state.backtracking>0 && alreadyParsedRule(input, 39) ) { return retval; }
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:352:3: ( '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR ->)
            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:352:5: '[' names+= VAR ( ',' names+= VAR )? ']' 'in' from= VAR
            {
            char_literal110=(Token)match(input,63,FOLLOW_63_in_arrayInput2034); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(char_literal110);

            names=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput2038); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(names);

            if (list_names==null) list_names=new ArrayList();
            list_names.add(names);

            // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:352:20: ( ',' names+= VAR )?
            int alt54=2;
            int LA54_0 = input.LA(1);

            if ( (LA54_0==31) ) {
                alt54=1;
            }
            switch (alt54) {
                case 1 :
                    // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:352:21: ',' names+= VAR
                    {
                    char_literal111=(Token)match(input,31,FOLLOW_31_in_arrayInput2041); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_31.add(char_literal111);

                    names=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput2045); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(names);

                    if (list_names==null) list_names=new ArrayList();
                    list_names.add(names);


                    }
                    break;

            }

            char_literal112=(Token)match(input,64,FOLLOW_64_in_arrayInput2049); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_64.add(char_literal112);

            string_literal113=(Token)match(input,42,FOLLOW_42_in_arrayInput2051); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_42.add(string_literal113);

            from=(Token)match(input,VAR,FOLLOW_VAR_in_arrayInput2055); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);

            if ( state.backtracking==0 ) {
               
                ((operator_scope)operator_stack.peek()).result.setInput(0, getBinding(from, JsonStreamExpression.class).getStream());
                for(int index = 0; index < list_names.size(); index++) {
              	  setBinding((Token) list_names.get(index), new InputSelection(index)); 
                }

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
            // 358:3: ->
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
            if ( state.backtracking>0 ) { memoize(input, 39, arrayInput_StartIndex); }
        }
        return retval;
    }
    // $ANTLR end "arrayInput"

    // $ANTLR start synpred8_SJaql
    public final void synpred8_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:117:5: ( ternaryExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:117:5: ternaryExpression
        {
        pushFollow(FOLLOW_ternaryExpression_in_synpred8_SJaql340);
        ternaryExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred8_SJaql

    // $ANTLR start synpred10_SJaql
    public final void synpred10_SJaql_fragment() throws RecognitionException {   
        SJaqlParser.orExpression_return ifClause = null;

        SJaqlParser.expression_return ifExpr = null;

        SJaqlParser.expression_return elseExpr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:4: (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:4: ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        {
        pushFollow(FOLLOW_orExpression_in_synpred10_SJaql357);
        ifClause=orExpression();

        state._fsp--;
        if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:26: ( '?' (ifExpr= expression )? ':' elseExpr= expression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:27: '?' (ifExpr= expression )? ':' elseExpr= expression
        {
        match(input,34,FOLLOW_34_in_synpred10_SJaql360); if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:121:37: (ifExpr= expression )?
        int alt56=2;
        int LA56_0 = input.LA(1);

        if ( ((LA56_0>=ID && LA56_0<=STRING)||(LA56_0>=DECIMAL && LA56_0<=UINT)||LA56_0==30||(LA56_0>=52 && LA56_0<=55)||LA56_0==58||(LA56_0>=60 && LA56_0<=63)||(LA56_0>=65 && LA56_0<=66)) ) {
            alt56=1;
        }
        switch (alt56) {
            case 1 :
                // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:0:0: ifExpr= expression
                {
                pushFollow(FOLLOW_expression_in_synpred10_SJaql364);
                ifExpr=expression();

                state._fsp--;
                if (state.failed) return ;

                }
                break;

        }

        match(input,35,FOLLOW_35_in_synpred10_SJaql367); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred10_SJaql371);
        elseExpr=expression();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred10_SJaql

    // $ANTLR start synpred11_SJaql
    public final void synpred11_SJaql_fragment() throws RecognitionException {   
        SJaqlParser.orExpression_return ifExpr2 = null;

        SJaqlParser.expression_return ifClause2 = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:4: (ifExpr2= orExpression 'if' ifClause2= expression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:123:4: ifExpr2= orExpression 'if' ifClause2= expression
        {
        pushFollow(FOLLOW_orExpression_in_synpred11_SJaql394);
        ifExpr2=orExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,36,FOLLOW_36_in_synpred11_SJaql396); if (state.failed) return ;
        pushFollow(FOLLOW_expression_in_synpred11_SJaql400);
        ifClause2=expression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred11_SJaql

    // $ANTLR start synpred32_SJaql
    public final void synpred32_SJaql_fragment() throws RecognitionException {   
        Token type=null;
        SJaqlParser.generalPathExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:171:5: ( '(' type= ID ')' expr= generalPathExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:171:5: '(' type= ID ')' expr= generalPathExpression
        {
        match(input,30,FOLLOW_30_in_synpred32_SJaql864); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred32_SJaql868); if (state.failed) return ;
        match(input,32,FOLLOW_32_in_synpred32_SJaql870); if (state.failed) return ;
        pushFollow(FOLLOW_generalPathExpression_in_synpred32_SJaql874);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred32_SJaql

    // $ANTLR start synpred33_SJaql
    public final void synpred33_SJaql_fragment() throws RecognitionException {   
        Token type=null;
        SJaqlParser.generalPathExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:172:4: (expr= generalPathExpression 'as' type= ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:172:4: expr= generalPathExpression 'as' type= ID
        {
        pushFollow(FOLLOW_generalPathExpression_in_synpred33_SJaql881);
        expr=generalPathExpression();

        state._fsp--;
        if (state.failed) return ;
        match(input,56,FOLLOW_56_in_synpred33_SJaql883); if (state.failed) return ;
        type=(Token)match(input,ID,FOLLOW_ID_in_synpred33_SJaql887); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred33_SJaql

    // $ANTLR start synpred34_SJaql
    public final void synpred34_SJaql_fragment() throws RecognitionException {   
        SJaqlParser.valueExpression_return value = null;

        SJaqlParser.pathExpression_return path = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:4: (value= valueExpression path= pathExpression )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:178:4: value= valueExpression path= pathExpression
        {
        pushFollow(FOLLOW_valueExpression_in_synpred34_SJaql921);
        value=valueExpression();

        state._fsp--;
        if (state.failed) return ;
        pushFollow(FOLLOW_pathExpression_in_synpred34_SJaql925);
        path=pathExpression();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred34_SJaql

    // $ANTLR start synpred37_SJaql
    public final void synpred37_SJaql_fragment() throws RecognitionException {   
        Token field=null;

        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:7: ( ( '.' (field= ID ) ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:7: ( '.' (field= ID ) )
        {
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:7: ( '.' (field= ID ) )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:8: '.' (field= ID )
        {
        match(input,57,FOLLOW_57_in_synpred37_SJaql1037); if (state.failed) return ;
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:12: (field= ID )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:192:13: field= ID
        {
        field=(Token)match(input,ID,FOLLOW_ID_in_synpred37_SJaql1042); if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred37_SJaql

    // $ANTLR start synpred38_SJaql
    public final void synpred38_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:193:11: ( arrayAccess )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:193:11: arrayAccess
        {
        pushFollow(FOLLOW_arrayAccess_in_synpred38_SJaql1060);
        arrayAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred38_SJaql

    // $ANTLR start synpred42_SJaql
    public final void synpred42_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:4: ( VAR )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:200:4: VAR
        {
        match(input,VAR,FOLLOW_VAR_in_synpred42_SJaql1099); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred42_SJaql

    // $ANTLR start synpred44_SJaql
    public final void synpred44_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:5: ( streamIndexAccess )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:202:5: streamIndexAccess
        {
        pushFollow(FOLLOW_streamIndexAccess_in_synpred44_SJaql1122);
        streamIndexAccess();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred44_SJaql

    // $ANTLR start synpred78_SJaql
    public final void synpred78_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:22: ( ',' input )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:304:22: ',' input
        {
        match(input,31,FOLLOW_31_in_synpred78_SJaql1899); if (state.failed) return ;
        pushFollow(FOLLOW_input_in_synpred78_SJaql1901);
        input();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred78_SJaql

    // $ANTLR start synpred79_SJaql
    public final void synpred79_SJaql_fragment() throws RecognitionException {   
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:307:1: ( operatorOption )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:307:1: operatorOption
        {
        pushFollow(FOLLOW_operatorOption_in_synpred79_SJaql1909);
        operatorOption();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred79_SJaql

    // $ANTLR start synpred82_SJaql
    public final void synpred82_SJaql_fragment() throws RecognitionException {   
        Token inputOption=null;
        SJaqlParser.contextAwareExpression_return expr = null;


        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:344:2: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )
        // /Users/arv/Proggn/Uni/PhD/private/simple/simple-jaql/src/main/java/eu/stratosphere/simple/jaql/SJaql.g:344:2: inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)]
        {
        inputOption=(Token)match(input,ID,FOLLOW_ID_in_synpred82_SJaql2004); if (state.failed) return ;
        if ( !(( (((input_scope)input_stack.peek()).inputProperty = findInputPropertyRelunctantly(((genericOperator_scope)genericOperator_stack.peek()).operatorInfo, inputOption)) != null )) ) {
            if (state.backtracking>0) {state.failed=true; return ;}
            throw new FailedPredicateException(input, "synpred82_SJaql", " ($input::inputProperty = findInputPropertyRelunctantly($genericOperator::operatorInfo, $inputOption)) != null ");
        }
        pushFollow(FOLLOW_contextAwareExpression_in_synpred82_SJaql2012);
        expr=contextAwareExpression(new InputSelection(((operator_scope)operator_stack.peek()).numInputs - 1));

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred82_SJaql

    // Delegated rules

    public final boolean synpred37_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred37_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred44_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred44_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred79_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred79_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred32_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred32_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred34_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred34_SJaql_fragment(); // can never throw exception
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
    public final boolean synpred10_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred10_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred11_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred11_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred78_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred78_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred33_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred33_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred38_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred38_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred42_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred42_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred82_SJaql() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred82_SJaql_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA5 dfa5 = new DFA5(this);
    protected DFA7 dfa7 = new DFA7(this);
    protected DFA22 dfa22 = new DFA22(this);
    protected DFA23 dfa23 = new DFA23(this);
    protected DFA26 dfa26 = new DFA26(this);
    protected DFA53 dfa53 = new DFA53(this);
    static final String DFA5_eotS =
        "\17\uffff";
    static final String DFA5_eofS =
        "\2\uffff\1\1\10\uffff\1\1\3\uffff";
    static final String DFA5_minS =
        "\1\6\1\uffff\1\6\1\uffff\1\7\1\6\2\0\1\6\1\11\2\6\1\11\1\6\1\0";
    static final String DFA5_maxS =
        "\1\102\1\uffff\1\104\1\uffff\1\14\1\104\2\0\1\102\1\100\1\102\2"+
        "\100\1\77\1\0";
    static final String DFA5_acceptS =
        "\1\uffff\1\1\1\uffff\1\2\13\uffff";
    static final String DFA5_specialS =
        "\6\uffff\1\0\1\1\6\uffff\1\2}>";
    static final String[] DFA5_transitionS = {
            "\1\2\2\1\1\uffff\3\1\21\uffff\1\1\25\uffff\4\1\2\uffff\1\1\1"+
            "\uffff\4\1\1\uffff\2\3",
            "",
            "\1\5\1\3\1\uffff\1\1\20\uffff\1\1\3\uffff\3\1\1\uffff\22\1"+
            "\4\uffff\2\1\1\uffff\1\1\3\uffff\1\4\1\1\3\uffff\1\3",
            "",
            "\1\3\1\uffff\1\1\1\uffff\2\1",
            "\1\6\1\7\1\1\1\uffff\3\1\21\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\3\1\1\10\1\uffff\2\1\1\uffff\1\3",
            "\1\uffff",
            "\1\uffff",
            "\1\1\1\11\1\1\1\uffff\3\1\21\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\4\1\1\uffff\2\1",
            "\1\1\25\uffff\1\12\2\uffff\1\1\1\uffff\20\1\4\uffff\2\1\5\uffff"+
            "\1\1\1\13",
            "\1\1\1\14\1\1\1\uffff\3\1\21\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\7\1",
            "\1\1\2\uffff\1\1\20\uffff\1\1\4\uffff\2\1\1\uffff\10\1\1\15"+
            "\11\1\4\uffff\2\1\1\uffff\1\1\3\uffff\2\1",
            "\1\1\25\uffff\1\1\2\uffff\1\1\1\uffff\20\1\4\uffff\2\1\5\uffff"+
            "\1\1\1\13",
            "\1\1\1\16\1\1\1\uffff\3\1\21\uffff\1\1\25\uffff\4\1\2\uffff"+
            "\1\1\1\uffff\4\1",
            "\1\uffff"
    };

    static final short[] DFA5_eot = DFA.unpackEncodedString(DFA5_eotS);
    static final short[] DFA5_eof = DFA.unpackEncodedString(DFA5_eofS);
    static final char[] DFA5_min = DFA.unpackEncodedStringToUnsignedChars(DFA5_minS);
    static final char[] DFA5_max = DFA.unpackEncodedStringToUnsignedChars(DFA5_maxS);
    static final short[] DFA5_accept = DFA.unpackEncodedString(DFA5_acceptS);
    static final short[] DFA5_special = DFA.unpackEncodedString(DFA5_specialS);
    static final short[][] DFA5_transition;

    static {
        int numStates = DFA5_transitionS.length;
        DFA5_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA5_transition[i] = DFA.unpackEncodedString(DFA5_transitionS[i]);
        }
    }

    class DFA5 extends DFA {

        public DFA5(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 5;
            this.eot = DFA5_eot;
            this.eof = DFA5_eof;
            this.min = DFA5_min;
            this.max = DFA5_max;
            this.accept = DFA5_accept;
            this.special = DFA5_special;
            this.transition = DFA5_transition;
        }
        public String getDescription() {
            return "116:1: expression : ( ternaryExpression | operatorExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA5_6 = input.LA(1);

                         
                        int index5_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred8_SJaql()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index5_6);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA5_7 = input.LA(1);

                         
                        int index5_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred8_SJaql()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index5_7);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA5_14 = input.LA(1);

                         
                        int index5_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred8_SJaql()) ) {s = 1;}

                        else if ( (true) ) {s = 3;}

                         
                        input.seek(index5_14);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 5, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA7_eotS =
        "\23\uffff";
    static final String DFA7_eofS =
        "\23\uffff";
    static final String DFA7_minS =
        "\1\6\17\0\3\uffff";
    static final String DFA7_maxS =
        "\1\77\17\0\3\uffff";
    static final String DFA7_acceptS =
        "\20\uffff\1\1\1\2\1\3";
    static final String DFA7_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14"+
        "\1\15\1\16\3\uffff}>";
    static final String[] DFA7_transitionS = {
            "\1\5\1\15\1\11\1\uffff\1\10\1\12\1\13\21\uffff\1\4\25\uffff"+
            "\1\1\1\2\2\3\2\uffff\1\17\1\uffff\1\6\1\7\1\14\1\16",
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
            return "120:1: ternaryExpression : (ifClause= orExpression ( '?' (ifExpr= expression )? ':' elseExpr= expression ) -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ifExpr2= orExpression 'if' ifClause2= expression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );";
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
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA7_2 = input.LA(1);

                         
                        int index7_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA7_3 = input.LA(1);

                         
                        int index7_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA7_4 = input.LA(1);

                         
                        int index7_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA7_5 = input.LA(1);

                         
                        int index7_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA7_6 = input.LA(1);

                         
                        int index7_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA7_7 = input.LA(1);

                         
                        int index7_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA7_8 = input.LA(1);

                         
                        int index7_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA7_9 = input.LA(1);

                         
                        int index7_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA7_10 = input.LA(1);

                         
                        int index7_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA7_11 = input.LA(1);

                         
                        int index7_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA7_12 = input.LA(1);

                         
                        int index7_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_12);
                        if ( s>=0 ) return s;
                        break;
                    case 12 : 
                        int LA7_13 = input.LA(1);

                         
                        int index7_13 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_13);
                        if ( s>=0 ) return s;
                        break;
                    case 13 : 
                        int LA7_14 = input.LA(1);

                         
                        int index7_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_14);
                        if ( s>=0 ) return s;
                        break;
                    case 14 : 
                        int LA7_15 = input.LA(1);

                         
                        int index7_15 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred10_SJaql()) ) {s = 16;}

                        else if ( (synpred11_SJaql()) ) {s = 17;}

                        else if ( (true) ) {s = 18;}

                         
                        input.seek(index7_15);
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
    static final String DFA22_eotS =
        "\20\uffff";
    static final String DFA22_eofS =
        "\20\uffff";
    static final String DFA22_minS =
        "\1\6\14\0\3\uffff";
    static final String DFA22_maxS =
        "\1\77\14\0\3\uffff";
    static final String DFA22_acceptS =
        "\15\uffff\1\1\1\2\1\3";
    static final String DFA22_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\3\uffff}>";
    static final String[] DFA22_transitionS = {
            "\1\2\1\12\1\6\1\uffff\1\5\1\7\1\10\21\uffff\1\1\33\uffff\1\14"+
            "\1\uffff\1\3\1\4\1\11\1\13",
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

    static final short[] DFA22_eot = DFA.unpackEncodedString(DFA22_eotS);
    static final short[] DFA22_eof = DFA.unpackEncodedString(DFA22_eofS);
    static final char[] DFA22_min = DFA.unpackEncodedStringToUnsignedChars(DFA22_minS);
    static final char[] DFA22_max = DFA.unpackEncodedStringToUnsignedChars(DFA22_maxS);
    static final short[] DFA22_accept = DFA.unpackEncodedString(DFA22_acceptS);
    static final short[] DFA22_special = DFA.unpackEncodedString(DFA22_specialS);
    static final short[][] DFA22_transition;

    static {
        int numStates = DFA22_transitionS.length;
        DFA22_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA22_transition[i] = DFA.unpackEncodedString(DFA22_transitionS[i]);
        }
    }

    class DFA22 extends DFA {

        public DFA22(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 22;
            this.eot = DFA22_eot;
            this.eof = DFA22_eof;
            this.min = DFA22_min;
            this.max = DFA22_max;
            this.accept = DFA22_accept;
            this.special = DFA22_special;
            this.transition = DFA22_transition;
        }
        public String getDescription() {
            return "171:4: ( '(' type= ID ')' expr= generalPathExpression | expr= generalPathExpression 'as' type= ID | expr= generalPathExpression )";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA22_1 = input.LA(1);

                         
                        int index22_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred32_SJaql()) ) {s = 13;}

                        else if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA22_2 = input.LA(1);

                         
                        int index22_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA22_3 = input.LA(1);

                         
                        int index22_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA22_4 = input.LA(1);

                         
                        int index22_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA22_5 = input.LA(1);

                         
                        int index22_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA22_6 = input.LA(1);

                         
                        int index22_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA22_7 = input.LA(1);

                         
                        int index22_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA22_8 = input.LA(1);

                         
                        int index22_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA22_9 = input.LA(1);

                         
                        int index22_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA22_10 = input.LA(1);

                         
                        int index22_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA22_11 = input.LA(1);

                         
                        int index22_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA22_12 = input.LA(1);

                         
                        int index22_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred33_SJaql()) ) {s = 14;}

                        else if ( (true) ) {s = 15;}

                         
                        input.seek(index22_12);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 22, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA23_eotS =
        "\17\uffff";
    static final String DFA23_eofS =
        "\17\uffff";
    static final String DFA23_minS =
        "\1\6\14\0\2\uffff";
    static final String DFA23_maxS =
        "\1\77\14\0\2\uffff";
    static final String DFA23_acceptS =
        "\15\uffff\1\1\1\2";
    static final String DFA23_specialS =
        "\1\uffff\1\0\1\1\1\2\1\3\1\4\1\5\1\6\1\7\1\10\1\11\1\12\1\13\2\uffff}>";
    static final String[] DFA23_transitionS = {
            "\1\1\1\12\1\6\1\uffff\1\5\1\7\1\10\21\uffff\1\2\33\uffff\1\14"+
            "\1\uffff\1\3\1\4\1\11\1\13",
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
            return "177:1: generalPathExpression : (value= valueExpression path= pathExpression -> | valueExpression );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA23_1 = input.LA(1);

                         
                        int index23_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_1);
                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA23_2 = input.LA(1);

                         
                        int index23_2 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_2);
                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA23_3 = input.LA(1);

                         
                        int index23_3 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_3);
                        if ( s>=0 ) return s;
                        break;
                    case 3 : 
                        int LA23_4 = input.LA(1);

                         
                        int index23_4 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_4);
                        if ( s>=0 ) return s;
                        break;
                    case 4 : 
                        int LA23_5 = input.LA(1);

                         
                        int index23_5 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_5);
                        if ( s>=0 ) return s;
                        break;
                    case 5 : 
                        int LA23_6 = input.LA(1);

                         
                        int index23_6 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_6);
                        if ( s>=0 ) return s;
                        break;
                    case 6 : 
                        int LA23_7 = input.LA(1);

                         
                        int index23_7 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_7);
                        if ( s>=0 ) return s;
                        break;
                    case 7 : 
                        int LA23_8 = input.LA(1);

                         
                        int index23_8 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_8);
                        if ( s>=0 ) return s;
                        break;
                    case 8 : 
                        int LA23_9 = input.LA(1);

                         
                        int index23_9 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_9);
                        if ( s>=0 ) return s;
                        break;
                    case 9 : 
                        int LA23_10 = input.LA(1);

                         
                        int index23_10 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_10);
                        if ( s>=0 ) return s;
                        break;
                    case 10 : 
                        int LA23_11 = input.LA(1);

                         
                        int index23_11 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_11);
                        if ( s>=0 ) return s;
                        break;
                    case 11 : 
                        int LA23_12 = input.LA(1);

                         
                        int index23_12 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred34_SJaql()) ) {s = 13;}

                        else if ( (true) ) {s = 14;}

                         
                        input.seek(index23_12);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 23, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA26_eotS =
        "\17\uffff";
    static final String DFA26_eofS =
        "\1\uffff\1\10\2\uffff\1\11\12\uffff";
    static final String DFA26_minS =
        "\2\6\2\uffff\1\6\5\uffff\1\6\2\43\1\uffff\1\0";
    static final String DFA26_maxS =
        "\1\77\1\100\2\uffff\1\100\5\uffff\1\77\2\100\1\uffff\1\0";
    static final String DFA26_acceptS =
        "\2\uffff\1\2\1\3\1\uffff\1\7\1\10\1\1\1\5\1\4\3\uffff\1\6\1\uffff";
    static final String DFA26_specialS =
        "\16\uffff\1\0}>";
    static final String[] DFA26_transitionS = {
            "\1\1\1\4\1\3\1\uffff\3\3\21\uffff\1\2\33\uffff\1\6\1\uffff\3"+
            "\3\1\5",
            "\1\10\2\uffff\1\10\20\uffff\1\10\3\uffff\1\7\2\10\1\uffff\22"+
            "\10\4\uffff\2\10\1\uffff\1\10\3\uffff\2\10",
            "",
            "",
            "\1\11\2\uffff\1\11\20\uffff\1\11\4\uffff\2\11\1\uffff\22\11"+
            "\4\uffff\2\11\1\uffff\1\11\3\uffff\1\12\1\11",
            "",
            "",
            "",
            "",
            "",
            "\3\15\1\11\1\15\1\13\1\14\21\uffff\1\15\33\uffff\1\15\1\uffff"+
            "\4\15",
            "\1\11\25\uffff\1\15\5\uffff\1\15\1\16",
            "\1\11\25\uffff\1\15\5\uffff\1\15\1\16",
            "",
            "\1\uffff"
    };

    static final short[] DFA26_eot = DFA.unpackEncodedString(DFA26_eotS);
    static final short[] DFA26_eof = DFA.unpackEncodedString(DFA26_eofS);
    static final char[] DFA26_min = DFA.unpackEncodedStringToUnsignedChars(DFA26_minS);
    static final char[] DFA26_max = DFA.unpackEncodedStringToUnsignedChars(DFA26_maxS);
    static final short[] DFA26_accept = DFA.unpackEncodedString(DFA26_acceptS);
    static final short[] DFA26_special = DFA.unpackEncodedString(DFA26_specialS);
    static final short[][] DFA26_transition;

    static {
        int numStates = DFA26_transitionS.length;
        DFA26_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA26_transition[i] = DFA.unpackEncodedString(DFA26_transitionS[i]);
        }
    }

    class DFA26 extends DFA {

        public DFA26(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 26;
            this.eot = DFA26_eot;
            this.eof = DFA26_eof;
            this.min = DFA26_min;
            this.max = DFA26_max;
            this.accept = DFA26_accept;
            this.special = DFA26_special;
            this.transition = DFA26_transition;
        }
        public String getDescription() {
            return "196:1: valueExpression : ( methodCall[null] | parenthesesExpression | literal | VAR -> | ID {...}? => -> | streamIndexAccess | arrayCreation | objectCreation );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA26_14 = input.LA(1);

                         
                        int index26_14 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred42_SJaql()) ) {s = 9;}

                        else if ( (synpred44_SJaql()) ) {s = 13;}

                         
                        input.seek(index26_14);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 26, _s, input);
            error(nvae);
            throw nvae;
        }
    }
    static final String DFA53_eotS =
        "\12\uffff";
    static final String DFA53_eofS =
        "\1\2\11\uffff";
    static final String DFA53_minS =
        "\1\6\1\0\10\uffff";
    static final String DFA53_maxS =
        "\1\100\1\0\10\uffff";
    static final String DFA53_acceptS =
        "\2\uffff\1\2\6\uffff\1\1";
    static final String DFA53_specialS =
        "\1\uffff\1\0\10\uffff}>";
    static final String[] DFA53_transitionS = {
            "\1\1\23\uffff\1\2\4\uffff\2\2\2\uffff\1\2\27\uffff\1\2\4\uffff"+
            "\1\2",
            "\1\uffff",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            ""
    };

    static final short[] DFA53_eot = DFA.unpackEncodedString(DFA53_eotS);
    static final short[] DFA53_eof = DFA.unpackEncodedString(DFA53_eofS);
    static final char[] DFA53_min = DFA.unpackEncodedStringToUnsignedChars(DFA53_minS);
    static final char[] DFA53_max = DFA.unpackEncodedStringToUnsignedChars(DFA53_maxS);
    static final short[] DFA53_accept = DFA.unpackEncodedString(DFA53_acceptS);
    static final short[] DFA53_special = DFA.unpackEncodedString(DFA53_specialS);
    static final short[][] DFA53_transition;

    static {
        int numStates = DFA53_transitionS.length;
        DFA53_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA53_transition[i] = DFA.unpackEncodedString(DFA53_transitionS[i]);
        }
    }

    class DFA53 extends DFA {

        public DFA53(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 53;
            this.eot = DFA53_eot;
            this.eof = DFA53_eof;
            this.min = DFA53_min;
            this.max = DFA53_max;
            this.accept = DFA53_accept;
            this.special = DFA53_special;
            this.transition = DFA53_transition;
        }
        public String getDescription() {
            return "344:1: (inputOption= ID {...}?expr= contextAwareExpression[new InputSelection($operator::numInputs - 1)] )?";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            TokenStream input = (TokenStream)_input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA53_1 = input.LA(1);

                         
                        int index53_1 = input.index();
                        input.rewind();
                        s = -1;
                        if ( (synpred82_SJaql()) ) {s = 9;}

                        else if ( (true) ) {s = 2;}

                         
                        input.seek(index53_1);
                        if ( s>=0 ) return s;
                        break;
            }
            if (state.backtracking>0) {state.failed=true; return -1;}
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 53, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

    public static final BitSet FOLLOW_statement_in_script124 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_script127 = new BitSet(new long[]{0x00000000080000C0L,0x0000000000000006L});
    public static final BitSet FOLLOW_statement_in_script129 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_26_in_script133 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_assignment_in_statement145 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_statement149 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_packageImport_in_statement153 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionDefinition_in_statement157 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_javaudf_in_statement161 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_27_in_packageImport176 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_packageImport180 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_assignment195 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_assignment197 = new BitSet(new long[]{0x0000000000000040L,0x0000000000000006L});
    public static final BitSet FOLLOW_operator_in_assignment201 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionDefinition223 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_functionDefinition225 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_29_in_functionDefinition227 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_functionDefinition229 = new BitSet(new long[]{0x0000000100000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition238 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_31_in_functionDefinition245 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_functionDefinition249 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_32_in_functionDefinition260 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_functionDefinition272 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_javaudf290 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_28_in_javaudf292 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_javaudf294 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_javaudf296 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_javaudf300 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_javaudf302 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expression_in_contextAwareExpression330 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_expression340 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_expression346 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression357 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_ternaryExpression360 = new BitSet(new long[]{0xF4F0000840001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_ternaryExpression364 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_ternaryExpression367 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_ternaryExpression371 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression394 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_ternaryExpression396 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_ternaryExpression400 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression422 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression435 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_37_in_orExpression439 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_38_in_orExpression443 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_andExpression_in_orExpression448 = new BitSet(new long[]{0x0000006000000002L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression482 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_39_in_andExpression486 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_40_in_andExpression490 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression495 = new BitSet(new long[]{0x0000018000000002L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression529 = new BitSet(new long[]{0x0000060000000002L});
    public static final BitSet FOLLOW_41_in_elementExpression534 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_elementExpression537 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression541 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression582 = new BitSet(new long[]{0x0001F80000000002L});
    public static final BitSet FOLLOW_43_in_comparisonExpression588 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_44_in_comparisonExpression594 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_45_in_comparisonExpression600 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_46_in_comparisonExpression606 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_47_in_comparisonExpression612 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_48_in_comparisonExpression618 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression623 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression703 = new BitSet(new long[]{0x0006000000000002L});
    public static final BitSet FOLLOW_49_in_arithmeticExpression709 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_50_in_arithmeticExpression715 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression720 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression763 = new BitSet(new long[]{0x0008000000000202L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression769 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_51_in_multiplicationExpression775 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression780 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_52_in_preincrementExpression821 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression823 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_53_in_preincrementExpression828 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression830 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression835 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_set_in_unaryExpression845 = new BitSet(new long[]{0xF4F0000040001DC0L});
    public static final BitSet FOLLOW_castExpression_in_unaryExpression854 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_castExpression864 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression868 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_castExpression870 = new BitSet(new long[]{0xF400000040001DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression874 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression881 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_castExpression883 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_castExpression887 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression894 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_generalPathExpression921 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_generalPathExpression925 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_generalPathExpression935 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression958 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_57_in_contextAwarePathExpression969 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_contextAwarePathExpression974 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_contextAwarePathExpression992 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_57_in_pathExpression1037 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_pathExpression1042 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathExpression1060 = new BitSet(new long[]{0x8200000000000002L});
    public static final BitSet FOLLOW_methodCall_in_valueExpression1081 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression1087 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression1093 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression1099 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_valueExpression1109 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_streamIndexAccess_in_valueExpression1122 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression1127 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression1133 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression1146 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_parenthesesExpression1167 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression1169 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_parenthesesExpression1171 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_methodCall1194 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_methodCall1196 = new BitSet(new long[]{0xF4F0000140001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_methodCall1203 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_31_in_methodCall1209 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_methodCall1213 = new BitSet(new long[]{0x0000000180000000L});
    public static final BitSet FOLLOW_32_in_methodCall1223 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1237 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1239 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1241 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1256 = new BitSet(new long[]{0x0200000810000002L});
    public static final BitSet FOLLOW_57_in_fieldAssignment1272 = new BitSet(new long[]{0x0000000000000240L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1275 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment1291 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_contextAwarePathExpression_in_fieldAssignment1309 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1312 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1316 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_35_in_fieldAssignment1350 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1354 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_28_in_fieldAssignment1366 = new BitSet(new long[]{0x0000000000000040L,0x0000000000000006L});
    public static final BitSet FOLLOW_operator_in_fieldAssignment1370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_objectCreation1413 = new BitSet(new long[]{0x08000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1416 = new BitSet(new long[]{0x0800000080000000L});
    public static final BitSet FOLLOW_31_in_objectCreation1419 = new BitSet(new long[]{0x00000000000000C0L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation1421 = new BitSet(new long[]{0x0800000080000000L});
    public static final BitSet FOLLOW_31_in_objectCreation1425 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_59_in_objectCreation1430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_60_in_literal1454 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_61_in_literal1470 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal1486 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal1502 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal1519 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UINT_in_literal1536 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_62_in_literal1551 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1565 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1567 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1569 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_arrayAccess1573 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1593 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1598 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1604 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1607 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayAccess1625 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1630 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1636 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_arrayAccess1639 = new BitSet(new long[]{0x0000000000001800L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1644 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1650 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayAccess1653 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_streamIndexAccess1681 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_63_in_streamIndexAccess1690 = new BitSet(new long[]{0xF400000040001DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_streamIndexAccess1694 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_streamIndexAccess1696 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayCreation1715 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1719 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayCreation1722 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_arrayCreation1726 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayCreation1730 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayCreation1733 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator1770 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator1774 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator1778 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_65_in_readOperator1792 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_readOperator1797 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1802 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_readOperator1808 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_readOperator1810 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_readOperator1814 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_readOperator1816 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_66_in_writeOperator1830 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_writeOperator1834 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_67_in_writeOperator1836 = new BitSet(new long[]{0x0000000000000140L});
    public static final BitSet FOLLOW_ID_in_writeOperator1841 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1846 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_writeOperator1852 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_30_in_writeOperator1854 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_STRING_in_writeOperator1858 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_writeOperator1860 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_genericOperator1880 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_operatorFlag_in_genericOperator1888 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_arrayInput_in_genericOperator1892 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_input_in_genericOperator1896 = new BitSet(new long[]{0x0000000080000042L});
    public static final BitSet FOLLOW_31_in_genericOperator1899 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_input_in_genericOperator1901 = new BitSet(new long[]{0x0000000080000042L});
    public static final BitSet FOLLOW_operatorOption_in_genericOperator1909 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_operatorOption1929 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_operatorOption1935 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_operatorFlag1956 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_68_in_input1978 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1986 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_input1988 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_input1994 = new BitSet(new long[]{0x0000000000000042L});
    public static final BitSet FOLLOW_ID_in_input2004 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_input2012 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_63_in_arrayInput2034 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput2038 = new BitSet(new long[]{0x0000000080000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_31_in_arrayInput2041 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput2045 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_64_in_arrayInput2049 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_arrayInput2051 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_VAR_in_arrayInput2055 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_synpred8_SJaql340 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred10_SJaql357 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_synpred10_SJaql360 = new BitSet(new long[]{0xF4F0000840001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_synpred10_SJaql364 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_synpred10_SJaql367 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_synpred10_SJaql371 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred11_SJaql394 = new BitSet(new long[]{0x0000001000000000L});
    public static final BitSet FOLLOW_36_in_synpred11_SJaql396 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_expression_in_synpred11_SJaql400 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_30_in_synpred32_SJaql864 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred32_SJaql868 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_32_in_synpred32_SJaql870 = new BitSet(new long[]{0xF400000040001DC0L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred32_SJaql874 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_synpred33_SJaql881 = new BitSet(new long[]{0x0100000000000000L});
    public static final BitSet FOLLOW_56_in_synpred33_SJaql883 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred33_SJaql887 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_synpred34_SJaql921 = new BitSet(new long[]{0x8200000000000000L});
    public static final BitSet FOLLOW_pathExpression_in_synpred34_SJaql925 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_57_in_synpred37_SJaql1037 = new BitSet(new long[]{0x0000000000000040L});
    public static final BitSet FOLLOW_ID_in_synpred37_SJaql1042 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_synpred38_SJaql1060 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred42_SJaql1099 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_streamIndexAccess_in_synpred44_SJaql1122 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_31_in_synpred78_SJaql1899 = new BitSet(new long[]{0x80000000000000C0L,0x0000000000000010L});
    public static final BitSet FOLLOW_input_in_synpred78_SJaql1901 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorOption_in_synpred79_SJaql1909 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred82_SJaql2004 = new BitSet(new long[]{0xF4F0000040001DC0L,0x0000000000000006L});
    public static final BitSet FOLLOW_contextAwareExpression_in_synpred82_SJaql2012 = new BitSet(new long[]{0x0000000000000002L});

}