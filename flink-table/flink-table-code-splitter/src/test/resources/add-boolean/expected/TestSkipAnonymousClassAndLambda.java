public class TestSkipAnonymousClassAndLambda {
boolean funHasReturned$0;
    public void fun(String a) {
        if (a.length() > 5) {
            a += "long";
            { funHasReturned$0 = true; return; }
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
        { funHasReturned$0 = true; return; }
    }

    private abstract class InnerClass {
        public abstract String getValue();
    }
}
