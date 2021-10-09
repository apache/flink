public class TestRewriteLocalVariableInForLoop2 {
int sum;
int local$0;

    public void myFun(int[] arr) {
         sum = 0;
        for (int item : arr) {local$0 = item;
            sum += local$0;
        }
        System.out.println(sum);
    }
}
