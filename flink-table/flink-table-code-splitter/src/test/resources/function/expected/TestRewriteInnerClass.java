public class TestRewriteInnerClass {
    public void myFun(int[] a, int[] b) throws RuntimeException {
        myFun_split0(a, b);

        myFun_split1(a, b);

        myFun_split2(a, b);

        myFun_split3(a, b);

        
        
    }
void myFun_split0(int[] a, int[] b) throws RuntimeException {
if (a[0] != 0) {
            a[0] += b[0];
            b[0] += a[1];
        }
}

void myFun_split1(int[] a, int[] b) throws RuntimeException {
a[1] += b[1];
b[1] += a[2];
}

void myFun_split2(int[] a, int[] b) throws RuntimeException {
if (a[2] != 0) {
            a[2] += b[2];
            b[2] += a[3];
        }
}

void myFun_split3(int[] a, int[] b) throws RuntimeException {
a[3] += b[3];
b[3] += a[4];
}


    public class InnerClass {
        public void myFun(int[] a, int[] b) throws RuntimeException {
            myFun_split4(a, b);

            myFun_split5(a, b);

            myFun_split6(a, b);

            myFun_split7(a, b);

            
            
        }
void myFun_split4(int[] a, int[] b) throws RuntimeException {
if (a[0] != 0) {
                a[0] += b[0];
                b[0] += a[1];
            }
}

void myFun_split5(int[] a, int[] b) throws RuntimeException {
a[1] += b[1];
b[1] += a[2];
}

void myFun_split6(int[] a, int[] b) throws RuntimeException {
if (a[2] != 0) {
                a[2] += b[2];
                b[2] += a[3];
            }
}

void myFun_split7(int[] a, int[] b) throws RuntimeException {
a[3] += b[3];
b[3] += a[4];
}

    }
}
