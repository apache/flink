public class TestSkipAnonymousClassAndLambda {
    public void fun(String a) {
        if (a.length() > 5) {
            a += "long";
            return;
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
        return;
    }

    private abstract class InnerClass {
        public abstract String getValue();
    }
}
