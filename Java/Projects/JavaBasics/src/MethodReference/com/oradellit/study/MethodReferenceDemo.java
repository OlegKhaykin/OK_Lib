package com.oradellit.study;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

// Demonstrating reference to a static method:
//
class IntegerListMapDemo
{
  private static class IntegerChecker
  {
    static boolean is_more_than(int val, int level)
    {
      return (val > level);
    }
  }

  // Let's define method "filter" using standard functional interface BiPredicate
  private static List<Integer> filter(List<Integer> list, BiPredicate<Integer, Integer> tester, Integer level)
  {
    ArrayList<Integer> lst = new ArrayList<>();
    
    for (Integer i: list) if (tester.test(i, level)) lst.add(i);
    
    return lst;
  }
  
  static void run()
  {
    List<Integer> list = Arrays.asList(1, 3, 5, 7, 9);
    
    // Both following ways are correct but the 2nd way looks more natural:
    // - 1st way, using lambda expression:
    for (Integer i: (filter(list, (a, b) -> IntegerChecker.is_more_than(a, b), 6))) System.out.println(i);
    // - 2nd way, using static method reference:
    for (Integer i: (filter(list, IntegerChecker::is_more_than, 7))) System.out.println(i);
  }
}

// Referencing instance method:
class GarageDemo
{
  class Car
  {
    private String make, model;
    private Integer year;
    
    Car(String p_make, String p_model, int p_year)
    {
      make = p_make; model = p_model; year = p_year;
    }
    
    public String toString()
    {
      return "Car(" + make + " " + model + " " + year + ")";
    }
  }

  private class Mechanic
  {
    private String name;
    
    private Mechanic(String p_name)
    {
      name = p_name;
    }
    
    private void fix(Car car)
    {
      System.out.println(name + ": fixing " + car);
    }
  }
  
  private void process(Car car, Consumer<Car> processor)
  {
    processor.accept(car);
  }
  
  void run()
  {
    Mechanic john = new Mechanic("John Smith");
    Mechanic bob = new Mechanic("Bob Brown");
    
    Car honda = new Car("Honda", "Civic", 2015);
    Car kia = new Car("KIA", "Sorrento", 2018);
    
    // Both following ways are valid:
    process(honda, c -> john.fix(c));
    process(kia, bob::fix); // No need to pass parameters to the method "fix" here; the method "process" will do it 
  }
}

public class MethodReferenceDemo
{
  public static void main(String[] args)
  {
    IntegerListMapDemo.run();
    new GarageDemo().run();
  }
}
