import java.util.logging.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.*;
/**
 * Created by konugantis on 5/19/2016.
 */
public class OracleDBConnection {

    List<String> queriesList = new ArrayList();
    PreparedStatement Statement = null;
    Logger log = Logger.getLogger(OracleDBConnection.class.getName());

    public void executeQuery(String host, String user, String password) throws IOException {

        File dbFile = new File("./src/test/resources/testdata/SQLQueries.txt");
        List<String> queries = getQueriesFromTextFile(dbFile);
        Connection conn = getDBConnection(host, user, password);
        System.out.println("test2");
        try {
            for (String sqlQuery : queries) {
                if (conn.isValid(10) && conn != null) {
                    Statement = conn.prepareStatement(sqlQuery);
                    int recordsUpdated = Statement.executeUpdate(sqlQuery);
                    if (recordsUpdated == 0) {
                        log.info("No Rows updated");
                    }
                } else {
                    log.info("not a valid connection");
                    break;
                }
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
            log.info("problems with connection");
        } finally {
            try {
                Statement.close();
            } catch (Exception e) {
                log.info("problems with connection");
                e.printStackTrace();
            }
            try {
                conn.close();
            } catch (Exception e) {
                log.info("problems with connection");
                e.printStackTrace();
            }
        }

    }


    public Connection getDBConnection(String host, String user, String password) {
        PreparedStatement Statement = null;
        Connection conn = null;
        String connectionString = "jdbc:oracle:thin:@" + host;
        Properties userProps = new Properties();
        userProps.setProperty("user", user);
        userProps.setProperty("password", password);
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(connectionString, userProps);
        } catch (SQLException e1) {
            log.info("problems with connection");
        } catch (ClassNotFoundException c1) {
            c1.printStackTrace();
        }
        return conn;
    }

  /*  public List<String> getQueriesFromTextFile(File dbFile) throws FileNotFoundException, IOException {


        BufferedReader br = new BufferedReader(new FileReader(dbFile));
        List<String> line = new ArrayList();
        List<Integer> lineswithUpdateString = new ArrayList();
        String lineReader = null;
        //Reading the lines of the file
        while ((lineReader = br.readLine()) != null) {
            line.add(lineReader);
        }
        br.close();


        //Reading the line numbers having the string "Update" in the line text
        for (int lineIterator = 0; lineIterator < line.size(); lineIterator++) {
            if (line.get(lineIterator).contains("Update")) {
                lineswithUpdateString.add(lineIterator);
            }
        }

        for (Integer linewithUpdateString : lineswithUpdateString) {
            int dataIterator = 2;
            String Query = line.get(linewithUpdateString);
            String genericQuery = Query;
            String Headerline = line.get(linewithUpdateString + 1);
            String[] Header1 = Headerline.split("\\|");
            while (linewithUpdateString + dataIterator < line.size() && !line.get(linewithUpdateString + dataIterator).toString().contains("Update")) {
                if (!line.get(linewithUpdateString + dataIterator).equals("")) {
                    String dataline = line.get(linewithUpdateString + dataIterator);

                    String[] dataline1 = dataline.split("\\|");

                    for (int itemIterator = 0; itemIterator < Header1.length; itemIterator++) {
                        genericQuery = genericQuery.replace(Header1[itemIterator], dataline1[itemIterator]);
                    }
                    queriesList.add(genericQuery);
                }

                dataIterator = dataIterator + 1;
                genericQuery = Query;
            }
        }
        return queriesList;
    }*/

    public List<String> getQueriesFromTextFile(File dbFile) throws FileNotFoundException, IOException {


        BufferedReader br = new BufferedReader(new FileReader(dbFile));
        List<String> line = new ArrayList();
        List<String> lineswithUpdateString = new ArrayList();
        String lineReader = null;
        //Reading the lines of the file
        while ((lineReader = br.readLine()) != null) {
            line.add(lineReader);
        }
        br.close();


        //Reading the line numbers having the string "Update" in the line text
        for (int lineIterator = 0; lineIterator < line.size(); lineIterator++) {
            if (line.get(lineIterator).contains("Update")) {
                lineswithUpdateString.add(line.get(lineIterator));
            }
        }
        return lineswithUpdateString;
    }

}
