class OuterClass
{
  private static String static_msg = "I am feeling good!";
  private String msg = "Hello";
  
  static class StaticClass
  {
    void display()
    {
      // Only static members of Outer class are accessible:
      System.out.println("Message from StaticClass: " + static_msg);  // OK
      // This would give a compilation error:
      // System.out.println("Message from StaticClass: " + msg);
    }
  }
  
  // Non-static nested class - also called Inner class
  class InnerClass
  {
    // Both static and non-static members of Outer class are accessible:
    void display()
    {
      System.out.println("Message from InnerClass: "+ msg + ", " + static_msg);
    }
  }
}

public class Main
{
  public static void main(String args[])
  {
    // We can create an instance of StaticClass
    // without first creating an instance of OuterClass:
    OuterClass.StaticClass staticObject = new OuterClass.StaticClass();
    staticObject.display();
    
    // To create an instance of InnerClass, we must first create an instance of OuterClass:
    OuterClass.InnerClass innerObject = new OuterClass().new InnerClass();
    innerObject.display();
  }
}
