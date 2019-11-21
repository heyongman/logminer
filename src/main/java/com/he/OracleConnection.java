package com.he;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *  
 */

public class OracleConnection {
    
    public Connection connect() throws SQLException{
        String host = "104.1.67.225";
        String port = "1521";
        String dbName = "ORCL";
        String user = "goldengate";
        String password = "goldengate";
        return DriverManager.getConnection("jdbc:oracle:thin:@"+host+":"+port+"/"+dbName, user, password);
    }
}