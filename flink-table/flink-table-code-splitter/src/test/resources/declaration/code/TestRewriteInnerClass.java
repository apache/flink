public class TestRewriteInnerClass {
    public void myFun() {
        int local1;
        String local2 = "AAAAA";
        final long local3;
        local3 = 100;
    }

    public class InnerClass {
        public void myFun() {
            int local1;
            local1 = 5;
            long local2;
            final String local3 = "BBBBB";
        }
    }
}
