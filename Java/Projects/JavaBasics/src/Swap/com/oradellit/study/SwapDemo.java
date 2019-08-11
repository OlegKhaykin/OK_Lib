package com.oradellit.study;

class Point
{
  private String name;
  private int x, y;
  
  Point(String name, int x, int y) {this.name = name; this.x = x; this.y = y; }
  
  @Override
  public String toString() {return name + "(" + x + "," + y + ")";}
}

class PointContainer
{
  Point p;
  PointContainer(Point point) {p = point;}
}

public class SwapDemo
{
  // Unfortunately, it is impossible in Java to implement a "regular swap" procedure.
  // Procedure parameters in Java are variables that are independent from the argument variables.
  // At the moment of invocation, the values of the argument variables are assigned to the corresponding parameter variables.
  // Changing the values of the parameter variables within the procedure body does not affect the original argument variables.
  // When procedure ends, its parameter variable are purged from memory with no effect on the argument variables.

  // For example:
  // This method operating on a primitive type int is useless.
  private static void swapInt(int i1, int i2)
  {
    int t = i1;
    i1 = i2;
    i2 = t;
  }
  // To prevent programmer from foolishly trying to change parameter values
  // it would be better to mark the parameters as final which is always the case in scala
  
  // This method operating on a complex type Point is useless as well.
  private static void swapPoints(Point p1, Point p2)
  {
    Point t = p1;
    p1 = p2;
    p2 = t;
  }
  
  // This trick works. However, here we are not swapping the arguments themselves,
  // we are swapping the values of their attributes.
  private static void swapContent(PointContainer pc1, PointContainer pc2)
  {
    Point temp = pc1.p;
    pc1.p = pc2.p;
    pc2.p = temp;
  }
  
  // This is a K-combinator
  private static Point K(Point a, Point b) {return a;}
  
  public static void main(String[] args)
  {
    // This does not work:
    int i1 = 1, i2 = 2;
    System.out.println("Integers before swap: "+ i1 + "; " + i2);
    swapInt(i1, i2);
    System.out.println("Integers after swap: "+ i1 + "; " + i2);
    System.out.println("So, it did not work :(\n");
  
    // This does not work:
    Point pA = new Point("A", 5, 10);
    Point pB = new Point("B", 1, 3);
    System.out.println("Points before swap: "+ pA + "; " + pB);
    swapPoints(pA, pB);
    System.out.println("Points after swap: "+ pA + "; " + pB); // the same output as before swap
    System.out.println("It did not work either :(\n");
  
    // This works:
    PointContainer pc1 = new PointContainer(pA);
    PointContainer pc2 = new PointContainer(pB);
    System.out.println("Content before swap: "+ pc1.p + "; " + pc2.p);
    swapContent(pc1, pc2);
    System.out.println("Content after swap: "+ pc1.p + "; " + pc2.p);
    System.out.println("It worked :)\n");
    
    // This works:
    System.out.println("Points before re-combination: "+ pA + "; " + pB);
    pB = K(pA, pA = pB);
    System.out.println("Points after re-combination: "+ pA + "; " + pB);
    System.out.println("It worked as well");
    
    // The previous method worked but it was cheating: re-combination happened outside the K procedure.
    // K actually did nothing except providing a temp space for swapping. We could simply do this:
    Point temp = pA;
    pA = pB;
    pB = temp;
    System.out.println("Points after re-combination: "+ pA + "; " + pB);
  }
}
