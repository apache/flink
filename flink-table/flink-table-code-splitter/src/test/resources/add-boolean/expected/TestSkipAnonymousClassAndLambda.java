public class TestSkipAnonymousClassAndLambda {
boolean codeSplitHasReturned$0;
    public void fun(String a) {
        if (a.length() > 5) {
            a += "long";
            { codeSplitHasReturned$0 = true; return; }
        }
        a =
                new InnerClass() {
                    @Override
                    public String getValue() {
                        return a + "test";
                    }
                }.getValue();
        java.util.function.Function<String, String> f =
                s -> {
                    s += "test";
                    return s;
                };
        a = f.apply(a);
        a += "short";
        { codeSplitHasReturned$0 = true; return; }
    }

    private abstract class InnerClass {
        public abstract String getValue();
    }
}
