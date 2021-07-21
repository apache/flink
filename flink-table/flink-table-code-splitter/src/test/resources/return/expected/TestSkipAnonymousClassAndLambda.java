public class TestSkipAnonymousClassAndLambda {
String funReturnValue$0;

    public String fun(int a, String b) { funImpl(a, b); return funReturnValue$0; }

void funImpl(int a, String b) {
        if (a > 0) {
            a += 5;
            { funReturnValue$0 = b + "test" + a; return; }
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
        { funReturnValue$0 = b + "test" + a; return; }
    }

    private abstract class InnerClass {

        public abstract String getValue();
    }
}
