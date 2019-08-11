package com.oradellit;

import java.sql.*;
import java.math.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.*;
import oracle.sql.*;

public class Main
{
  public static void main(String[] args) throws java.sql.SQLException
  {
    OracleDataSource ods = new OracleDataSource();
    ods.setURL("jdbc:oracle:thin:@//PowerEdge:1521/pdb1.oitc.com");
    ods.setUser("HR");
    ods.setPassword("m");
    Connection conn = ods.getConnection();
    
    System.out.println("Connection opened");
  
    Statement stmt = conn.createStatement ();
    stmt.executeQuery("begin foo; end;");
    
    while (stmt.getMoreResults())
    {
      ResultSet rs = stmt.getResultSet();
      System.out.println("ResultSet");
      while (rs.next())
      {
        /* get results */
      }
    }
    
    conn.close();
    System.out.println("Connection closed");
  }
}
