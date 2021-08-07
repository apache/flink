public class TestRewriteLocalVariableInForLoop1 {
    public void myFun() {
        int sum = 0;
        for (int i = 0; i < 100; i++) {
            sum += i;
        }
        System.out.println(sum);
    }
}
