package com.oradellit.study;

public class DataTypes
{
  private enum CountersEnum { ONE, TWO, THREE }
  
  public static void main(String[] args)
  {
    CountersEnum n = CountersEnum.ONE;
    
    int i = 1;
    float f = i; // This is how to convert int to float in Java 5 and higher
    
    System.out.println(i);
    System.out.println(n);
  }
}
