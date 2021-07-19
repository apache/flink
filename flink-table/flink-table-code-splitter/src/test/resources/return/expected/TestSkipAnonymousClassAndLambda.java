public class TestSkipAnonymousClassAndLambda {
String codeSplitReturnValue$0;
    public String fun(int a, String b) { fun_impl(a, b); return codeSplitReturnValue$0; }

void fun_impl(int a, String b) {
        if (a > 0) {
            a += 5;
            { codeSplitReturnValue$0 = b + "test" + a; return; }
        }
        b =
                new InnerClass() {
                    @Override
                    public String getValue() {
                        return b + "test";
                    }
                }.getValue();
        java.util.function.Function<String, String> f =
                s -> {
                    s += "test" + a;
                    return s;
                };
        b = f.apply(b);
        a -= 5;
        { codeSplitReturnValue$0 = b + "test" + a; return; }
    }

    private abstract class InnerClass {

        public abstract String getValue();
    }
}
