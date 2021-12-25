public class TestRewriteInnerClass {
    public void myFun(int[] a, int[] b) {
        if (a[0] == 0) {
myFun_trueFilter1(a, b);

}
 else {
myFun_falseFilter2(a, b);

}

    }
void myFun_trueFilter1(int[] a, int[] b){
            a[0] += b[0];
            a[1] += b[1];
        }


void myFun_falseFilter2(int[] a, int[] b){
            a[0] += b[1];
            a[1] += b[0];
        }



    public class InnerClass {
        public void myFun(int[] a, int[] b) {
            if (a[0] == 0) {
myFun_trueFilter3(a, b);

}
 else {
myFun_falseFilter4(a, b);

}

        }
void myFun_trueFilter3(int[] a, int[] b){
                a[0] += b[0];
                a[1] += b[1];
            }


void myFun_falseFilter4(int[] a, int[] b){
                a[0] += b[1];
                a[1] += b[0];
            }


    }
}
