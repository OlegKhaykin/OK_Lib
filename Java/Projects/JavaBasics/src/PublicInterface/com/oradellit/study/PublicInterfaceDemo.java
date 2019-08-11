package com.oradellit.study;

interface MyInterface
{
  final public String myWord = "Hello"; // You may skip the word "final", the field will be final anyway
  
  default public void sayMyWord() // This is a new feature of Java 8: default Interface method
  {
    System.out.println(myWord);
  }
}

abstract class MyAbstractClass implements MyInterface
{
  void doSomething()
  {
    System.out.println("Doing something");
    // myWord = "Privet"; // Error: you cannot change a final field
  };
}

class MyImplementation extends MyAbstractClass
{
  @Override
  void doSomething() {}   // do nothing
}

public class PublicInterfaceDemo
{
  public static void main(String[] args)
  {
    MyAbstractClass obj = new MyImplementation();
    obj.doSomething();
    obj.sayMyWord();      // calling default method of MyInterface
  }
}