public class TestRewriteLocalVariableInForLoop1 {
int sum;
int i;

    public void myFun() {
         sum = 0;
        for ( i = 0; i < 100; i++) {
            sum += i;
        }
        System.out.println(sum);
    }
}
