package com.oradellit.study;

import java.util.Scanner;

public class Strings
{
  public static void main(String[] args)
  {
    String str1 = "Hello"; // new String "Hello" has been created in memory; str1 holds a reference to that memory area; Hello.RC=1
    String str2 = str1;    // str2 also holds the same reference to "Hello"; Hello.RC=2
    str1 = "Privet";       // new String "Privet" has been created; str1 -> "Privet"; str2 -> "Hello"; Privet.RC=1; Hello.RC=1
    
    str1 = str2;           // str1 -> "Hello" and str2 -> "Hello"; Hello.RC=2; Privet.RC=0; "Privet" will be purged from memory by JVM garbage collector
    
    System.out.println("Variables are " + ((str1 == str2) ? "equal" : "not equal")); // Variables are equal
    
    
    str2 = "Hel"+"lo";   // Let's try to trick the compiler and create a different String object with the same value "Hello"   
    // Java compiler has figured-out that "Hel"+"lo" = "Hello" and did not create a new String; str2 still references the same old "Hello" whose RC is still 2 
    System.out.println("Variables are " + ((str1 == str2) ? "equal" : "not equal")); // Variables are equal 
    
    Scanner s = new Scanner(System.in);
    
    while (true)
    {
      System.out.print("Enter a String: ");
      
      str2 = s.nextLine();
      
      if (str2.equals("")) break;
      
      System.out.println("You've entered \"" + str2 + "\"");
      System.out.println("Variables are " + ((str1 == str2) ? "equal" : "not equal")); // Variables are not equal
      System.out.println("Values are " + (str2.equals(str1) ? "equal" : "not equal")); // Values are equal if the user enters "Hello"
    }
    
    s.close();
    
    
    System.out.println("Good bye" + String.valueOf(5));
  }
}
