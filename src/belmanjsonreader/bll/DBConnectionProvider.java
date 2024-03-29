/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package belmanjsonreader.bll;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import java.sql.Connection;

/**
 *
 * @author
 */
public class DBConnectionProvider {

    //Login details with EASV database server ip, db name, user and password.
    private static final String SERVER_NAME = "10.176.111.31";
    private static final String DATABASE_NAME = "BelManProject"; 
    private static final String USER = "CS2018A_5";
    private static final String PASSWORD = "CS2018A_5";

    private final SQLServerDataSource db;

    //tries to establish a connection to EASV database | Requires Tryllehat if outside EASV school. 
    public DBConnectionProvider() {
        db = new SQLServerDataSource();
        db.setServerName(SERVER_NAME);
        db.setDatabaseName(DATABASE_NAME);
        db.setUser(USER);
        db.setPassword(PASSWORD);
    }

    public Connection getConnection() throws SQLServerException {
        Connection con = db.getConnection();
        return con;
    }

}
