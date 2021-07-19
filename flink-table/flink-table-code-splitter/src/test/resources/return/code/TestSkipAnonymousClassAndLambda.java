public class TestSkipAnonymousClassAndLambda {
    public String fun(int a, String b) {
        if (a > 0) {
            a += 5;
            return b + "test" + a;
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
        return b + "test" + a;
    }

    private abstract class InnerClass {
        public abstract String getValue();
    }
}
