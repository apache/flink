public class TestRewriteLocalVariableInForLoop2 {
    public void myFun(int[] arr) {
        int sum = 0;
        for (int item : arr) {
            sum += item;
        }
        System.out.println(sum);
    }
}
