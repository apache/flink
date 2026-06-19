public class TestRewriteInnerClass {
boolean myFunHasReturned$0;
    public void myFun(int[] a, int[] b) throws RuntimeException {
myFunHasReturned$0 = false;
        myFun_split2(a, b);
if (myFunHasReturned$0) { return; }

        myFun_split3(a, b);

        myFun_split4(a, b);
if (myFunHasReturned$0) { return; }

        myFun_split5(a, b);

        
        
    }
void myFun_split2(int[] a, int[] b) throws RuntimeException {
if (a[0] != 0) {
            a[0] += b[0];
            b[0] += a[1];
            { myFunHasReturned$0 = true; return; }
        }
}

void myFun_split3(int[] a, int[] b) throws RuntimeException {
a[1] += b[1];
b[1] += a[2];
}

void myFun_split4(int[] a, int[] b) throws RuntimeException {
if (a[2] != 0) {
            a[2] += b[2];
            b[2] += a[3];
            { myFunHasReturned$0 = true; return; }
        }
}

void myFun_split5(int[] a, int[] b) throws RuntimeException {
a[3] += b[3];
b[3] += a[4];
}


    public class InnerClass {
boolean myFunHasReturned$1;
        public void myFun(int[] a, int[] b) throws RuntimeException {
myFunHasReturned$1 = false;
            myFun_split6(a, b);
if (myFunHasReturned$1) { return; }

            myFun_split7(a, b);

            myFun_split8(a, b);
if (myFunHasReturned$1) { return; }

            myFun_split9(a, b);

            
            
        }
void myFun_split6(int[] a, int[] b) throws RuntimeException {
if (a[0] != 0) {
                a[0] += b[0];
                b[0] += a[1];
                { myFunHasReturned$1 = true; return; }
            }
}

void myFun_split7(int[] a, int[] b) throws RuntimeException {
a[1] += b[1];
b[1] += a[2];
}

void myFun_split8(int[] a, int[] b) throws RuntimeException {
if (a[2] != 0) {
                a[2] += b[2];
                b[2] += a[3];
                { myFunHasReturned$1 = true; return; }
            }
}

void myFun_split9(int[] a, int[] b) throws RuntimeException {
a[3] += b[3];
b[3] += a[4];
}

    }
}
