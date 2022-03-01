public class TestSplitJavaCode {
boolean[] rewrite$14 = new boolean[2];
int[][] rewrite$15 = new int[1][];
int[] rewrite$16 = new int[6];

{
rewrite$16[5] = 1;
}










    
    

    public TestSplitJavaCode(int[] b) {
        this.rewrite$15[0] = b;
    }

    public void myFun1(int a) {
rewrite$14[0] = false;
        myFun1_split6(a);

        myFun1_split7(a);

        myFun1_split8(a);

        myFun1_split9(a);
if (rewrite$14[0]) { return; }

         
         
         
        

        
    }
void myFun1_split6(int a) {

this.rewrite$16[5] = a;
rewrite$15[0][0] += rewrite$15[0][1];
rewrite$15[0][1] += rewrite$15[0][2];
rewrite$15[0][2] += rewrite$15[0][3];
rewrite$16[0] = rewrite$15[0][0] + rewrite$15[0][1];
rewrite$16[1] = rewrite$15[0][1] + rewrite$15[0][2];
}

void myFun1_split7(int a) {
rewrite$16[2] = rewrite$15[0][2] + rewrite$15[0][0];
}

void myFun1_split8(int a) {
for (int x : rewrite$15[0]) {rewrite$16[3] = x;
            System.out.println(rewrite$16[3] + rewrite$16[0] + rewrite$16[1] + rewrite$16[2]);
        }
}

void myFun1_split9(int a) {
if (rewrite$16[0] > 0) {
myFun1_trueFilter3(a);

}
 else {
            rewrite$15[0][0] += 50;
            rewrite$15[0][1] += 100;
            rewrite$15[0][2] += 150;
            rewrite$15[0][3] += 200;
            rewrite$15[0][4] += 250;
            rewrite$15[0][5] += 300;
            rewrite$15[0][6] += 350;
            { rewrite$14[0] = true; return; }
        }
}

void myFun1_trueFilter3(int a){
            myFun1_trueFilter3_split10(a);

            myFun1_trueFilter3_split11(a);

        }
void myFun1_trueFilter3_split10(int a) {

rewrite$15[0][0] += 100;
}

void myFun1_trueFilter3_split11(int a) {
if (rewrite$16[1] > 0) {
                rewrite$15[0][1] += 100;
                if (rewrite$16[2] > 0) {
                    rewrite$15[0][2] += 100;
                } else {
                    rewrite$15[0][2] += 50;
                }
            } else {
                rewrite$15[0][1] += 50;
            }
}




    public int myFun2(int[] a) { myFun2Impl(a); return rewrite$16[4]; }

void myFun2Impl(int[] a) {
rewrite$14[1] = false;
        myFun2Impl_split12(a);

        myFun2Impl_split13(a);
if (rewrite$14[1]) { return; }

        
        
        
        
    }
void myFun2Impl_split12(int[] a) {

a[0] += 1;
a[1] += 2;
a[2] += 3;
a[3] += 4;
a[4] += 5;
}

void myFun2Impl_split13(int[] a) {
{ rewrite$16[4] = a[0] + a[1] + a[2] + a[3] + a[4]; { rewrite$14[1] = true; return; } }
}

}
