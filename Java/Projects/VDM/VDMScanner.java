import java.sql.*;
 
public class VDMScanner
{
  private static String ora_url = "jdbc:default:connection";
  private static String ora_usr;
  private static String ora_pwd;
  private static Connection ora;
  private static Connection rmt;
  private static CallableStatement cs_open;
  private static CallableStatement cs_begin;
  private static CallableStatement cs_end;
  private static CallableStatement cs_close;
  private static CallableStatement cs_error_out;
  private static int n_log_level;
 
  // This main method is for client-side testing only:
  public static void main(String[] args) throws Exception
  {
    Class.forName("oracle.jdbc.OracleDriver");
    ora_url = "jdbc:oracle:thin:@epslnxswdb3d:11150:UALMDVG";
    ora_usr = "ual_vdm";
    ora_pwd = "Lapiet07";
   
    System.out.println("Started");
//  getMetadata("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@epslnxswdb3d:11150:UALMDVG", "ual_fed", "October09", "UAL_MNG","");
//  getMetadata("org.netezza.Driver", "jdbc:netezza://169.193.157.137/UAL_RPTVIEW_D2", "GEN_UAL_DEV", "Z31PoB3W", "GEN_UAL_DEV","");
    getMetadata("com.teradata.jdbc.TeraDriver", "jdbc:teradata://edwdevcop1.nam.nsroot.net/database=UAL_NA_DEP_QA,TMODE=TERA,CHARSET=ASCII", "UAL_NA_DEP_QA", "U59100al", "UAL_NA_DEP_QA", "N", "ACTG_UNIT_BAL_FACT");
    ora.commit();
    System.out.println("Done");
  }
 
  private static void init() throws Exception
  {
    cs_open = ora.prepareCall("begin ual_mng.xl.open(?, TRUE, ?); end;");
    cs_begin = ora.prepareCall("begin ual_mng.xl.begin_action(?, ?); end;");
    cs_end = ora.prepareCall("begin ual_mng.xl.end_action(?); end;");
    cs_close = ora.prepareCall("begin ual_mng.xl.close(?, ?, FALSE); end;");
    cs_error_out = ora.prepareCall("begin ual_mng.xl.close(?, ?, TRUE); end;");
  }
 
  private static void open_log(String p_description) throws Exception
  {
    cs_open.setString(1, p_description);
    cs_open.registerOutParameter(2, java.sql.Types.INTEGER);
    cs_open.execute();
    n_log_level = cs_open.getInt(2);
  }
 
  private static void close_log(String p_result) throws Exception
 {
    cs_close.setString(1, p_result);
    cs_close.setInt(2, n_log_level);
    cs_close.execute();
  }
 
  private static void error_out(String p_result) throws Exception
  {
    cs_error_out.setString(1, p_result);
    cs_error_out.setInt(2, n_log_level);
    cs_error_out.execute();
  }
 
  private static void begin_action(String p_action, String p_comment) throws Exception
  {
    cs_begin.setString(1, p_action);
    cs_begin.setString(2, p_comment);
    cs_begin.execute();
  }
 
  private static void end_action(String p_comment) throws Exception
  {
    cs_end.setString(1, p_comment);
    cs_end.execute();
  }
 
  // This getMetadata method is exposed as PL/SQL procedure UAL_VDM.GET_METADATA
  public static void getMetadata(String p_driver_class, String p_jdbc_url, String p_usr, String p_pwd, String p_schema, String p_check_valid, String p_tlist) throws Exception
  {
    String qry;
    String tname;
    Statement stm1;
    ResultSet rs1;
   
    try
    {
      ora = DriverManager.getConnection(ora_url, ora_usr, ora_pwd);
      ora.setAutoCommit(false);
      init();
 
      open_log("JAVA: VDMScanner.getMetadata");
     
      begin_action("Connecting to remote DB", "");
        Class.forName(p_driver_class);
        rmt = DriverManager.getConnection(p_jdbc_url, p_usr, p_pwd);
        String db_type = rmt.getMetaData().getDatabaseProductName();
        stm1 = rmt.createStatement();
      end_action("Connected to "+db_type);
       
      begin_action("Collecting metadata","");
        if (db_type.startsWith("Teradata"))
        {
          qry = "SELECT "
              + "CASE t.tablekind WHEN 'V' THEN 'VIEW' ELSE 'TABLE' END AS table_type, "
              + "RTRIM(t.tablename) AS table_name, "
              + "RTRIM(c.columnname) AS column_name, "
              + "c.columntype AS data_type, "
              + "c.columnlength AS max_length, "
              + "c.DecimalTotalDigits AS max_digits, "
              + "c.DecimalFractionalDigits AS scale, "
              + "NULL AS status "
              + "FROM dbc.tables t "
              + "JOIN dbc.columns c ON c.databasename = t.databasename AND c.tablename = t.tablename "
              + "WHERE t.DatabaseName = '"+p_schema+"'";
         
          tname = "t.TableName";
        }
        else if (db_type.startsWith("Netezza"))
        {
          qry = "SELECT type AS table_type, name AS table_name, attname AS column_name, format_type AS data_type, "
              + "-1 AS max_length, -1 AS max_digits, -1 AS scale, NULL AS status "
              + "FROM _V_RELATION_COLUMN "
              + "WHERE database ='"+p_schema+"'";
         
          tname = "name";
        }
        else if (db_type.startsWith("Oracle"))
        {
          qry = "SELECT t.object_type AS table_type, tc.table_name, tc.column_name, tc.data_type, tc.precision, tc.scale, t.status "
              + "FROM all_tab_columns tc "
              + "JOIN all_objects t ON t.owner = tc.owner AND t.object_name = tc.table_name "
              + "WHERE tc.owner = '"+p_schema+"'";
         
          tname = "table_name";
        }
        else if (db_type.startsWith("Sybase"))
        {
          qry = "SELECT so.name AS table_name, sc.name AS column_name, "
              + "st.name AS data_type, sc.length AS max_length, sc.prec AS max_digits, sc.scale, NULL AS status "
              + "FROM sysusers su "
              + "INNER JOIN sysobjects so ON so.uid = su.uid AND so.type IN ('U', 'V') "
              + "INNER JOIN syscolumns sc ON sc.id = so.id "
              + "INNER JOIN systypes st ON st.usertype = sc.usertype "
              + "WHERE su.name = '"+p_schema+"'";
         
          tname = "so.name";
        }
        else throw(new Exception("Wrong DB type: "+db_type));
       
        if (p_tlist != null && !p_tlist.equals("")) qry = qry + " AND "+tname+" IN('"+p_tlist.replace(",", "','")+"')";
        qry = qry + " ORDER BY table_name";
       
        stm1.setFetchSize(1000);
        rs1 = stm1.executeQuery(qry);
       
        begin_action("Storing metadata in the Oracle table TMP_TABLE_COLUMNS", "");
          PreparedStatement ps = ora.prepareStatement
          (
            "INSERT INTO ual_vdm.tmp_table_columns(table_type, table_name, column_name, data_type, max_length, precision, scale, status)" +
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
          );
         
          begin_action("Adding rows", "");
            int i = 0; int total = 0;
             
            while (rs1.next())
            {
              ps.setString(1, rs1.getString(1));  // table type
              ps.setString(2, rs1.getString(2));  // table name
              ps.setString(3, rs1.getString(3));  // column_name
              ps.setString(4, rs1.getString(4));  // column data type
              ps.setInt(5, rs1.getInt(5));        // column max length
              ps.setInt(6, rs1.getInt(6));        // column precision
              ps.setInt(7, rs1.getInt(7));        // column scale
              ps.setString(8, rs1.getString(8));  // view status
             
              ps.addBatch(); // adding rows to the insert batch (not inserting yet!)
              i++; total++;
             
              if (i==1000)
              {
                ps.executeBatch();  // insert 1000 rows into Oracle table
                end_action("1000 rows added");
                begin_action("... continue ...", "");
                i = 0;
              }
            }
            if (i > 0)
            {
              ps.executeBatch(); // insert remaining rows into Oracle table
              end_action(i + " rows added");
            }
            else
          end_action("Nothing to add anymore");
          rs1.close();
        end_action("Total rows created: "+total);
       
        if (p_check_valid.equals("Y"))
        {
          begin_action("Checking validity of the found views", "");
            stm1 = ora.createStatement();
            Statement stm2 = rmt.createStatement();
            ResultSet rs2;
            ResultSetMetaData rsmd;
            int numColumns;
           
            ps = ora.prepareStatement
            (
              "UPDATE ual_vdm.tmp_table_columns " +
              "SET data_type = ?, max_length = ?, precision = ?, scale = ?, status= ? " +
              "WHERE table_name = ? AND column_name = NVL(?, column_name)"
            );
           
            rs1 = stm1.executeQuery("SELECT DISTINCT table_name FROM ual_vdm.tmp_table_columns "
                + "WHERE table_type = 'VIEW' AND (status IS NULL OR data_type IS NULL) "
                + "ORDER BY table_name");
           
            while (rs1.next())
            {
              tname = rs1.getString(1);
              ps.setString(6, tname);
             
              try
              {
                begin_action("Checking view", tname);
                  rs2 = stm2.executeQuery("SELECT * FROM "+p_schema+"."+tname+" WHERE 1=2");
                  ps.setString(5, "VALID");
                  rsmd = rs2.getMetaData();
                  numColumns = rsmd.getColumnCount();
                 
                  for (int j=1; j<=numColumns; j++)
                  {
                    ps.setString(1, rsmd.getColumnTypeName(j));
                    ps.setInt(2, rsmd.getColumnDisplaySize(j));
                    ps.setInt(3, rsmd.getPrecision(j));
                    ps.setInt(4, rsmd.getScale(j));
                    ps.setString(7, rsmd.getColumnName(j));
                    ps.executeUpdate();
                  }
                  rs2.close();
                end_action("View is valid");
              }
              catch(Exception err)
              {
                ps.setString(5, "INVALID");
                ps.setString(7, ""); // all columns
                ps.executeUpdate();
                end_action("View is invalid");
              }
            }
            rs2 = null; stm2 = null;
          end_action(""); // End of checking validity of the found views
        }
      end_action(""); // End of Collecting meta-data
     
      rs1.close(); rs1 = null; stm1 = null;
      close_log("Successfully completed");
    }
    catch (Exception e)
    {
      error_out(e.getMessage());
      throw(e);
    }
  } // end of getMetadata method
}
