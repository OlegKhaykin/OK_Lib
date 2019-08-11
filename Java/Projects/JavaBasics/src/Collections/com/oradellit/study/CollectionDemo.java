package com.oradellit.study;

import java.awt.Color;
import java.util.Arrays;
import java.util.List;

class Shape
{
  private String name;
  private Color color;
  
  Shape(String name, Color color)
  {
    this.name = name;
    this.color = color;
  }
  
  String getName()
  {
    return name;
  }
  
  Color getColor()
  {
    return color;
  }
}

public class CollectionDemo
{
  public static void main(String[] args)
  {
    Shape[] shapes = {new Shape("cube", Color.BLUE), new Shape("circle", Color.RED), new Shape("triangle", Color.RED)}; 
    
    // Let's create a List "wrapper" around the base array.
    // Note: the wrapper operates on the base array, it does not create a new copy of data. This is great!
    List<Shape> my_shapes = Arrays.asList(shapes);
    
    // Let's use new cool Java 8 features: stream, filter, map, forEach, lambda expressions and method reference:
    my_shapes.stream().filter(s -> s.getColor() == Color.RED).map(s -> s.getName()).forEach(System.out::println);
  }
}