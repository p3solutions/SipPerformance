package com.p3.archon.sip_process.core;

import com.p3.archon.sip_process.bean.InputArgs;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;

/**
 * Created by Suriyanarayanan K
 * on 29/02/20 12:48 PM.
 */
public class ConnectionChecker {

    Connection con;
    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    public String getConnectionString(InputArgs inputArgs) {
        Object[] replaceValues = new Object[]{inputArgs.getHost(), inputArgs.getPort(), inputArgs.getDatabase()};
        return MessageFormat.format(getConnectionURL(inputArgs.getDatabaseServer().toLowerCase()), replaceValues);
    }

    public String getConnectionURL(String type) {
        if ("teradata".equals(type)) {
            return "jdbc:teradata://{0}/{2}";
        } else if ("sqlwinauth".equals(type)) {
            return "jdbc:jtds:sqlserver://localhost/{2};appName=SchemaCrawler;useCursors=true;useNTLMv2=true;domain={0};";
        } else if ("sql".equals(type)) {
            return "jdbc:jtds:sqlserver://{0}:{1}/{2};appName=SchemaCrawler;useCursors=true;";
        } else if ("oracle".equals(type)) {
            return "jdbc:oracle:thin:@{0}:{1}:{2}";
        } else if ("oracleservice".equals(type)) {
            return "jdbc:oracle:thin:@//{0}:{1}/{2}";
        } else if ("mysql".equals(type)) {
            return "jdbc:mysql://{0}:{1}/{2}?nullNamePatternMatchesAll=true&logger=Jdk14Logger&dumpQueriesOnException=true&dumpMetadataOnColumnNotFound=true&maxQuerySizeToLog=4096&useSSL=false";
        } else if ("db2".equals(type)) {
            return "jdbc:db2://{0}:{1}/{2};retrieveMessagesFromServerOnGetMessage=true;";
        } else if ("sybase".equals(type)) {
            return "jdbc:jtds:sybase://{0}:{1}/{2}";
        } else if ("postgresql".equals(type)) {
            return "jdbc:postgresql://{0}:{1}/{2}?ApplicationName=SchemaCrawler";
        } else if ("as400".equals(type)) {
            return "jdbc:as400://{0}:{1}/{2};translate hex=character;translate binary=true";
        } else if ("as400noport".equals(type)) {
            return "jdbc:as400://{0}/{2};translate hex=character;translate binary=true";
        }
        return null;
    }

    public String getForName(String serverType) {
        String driverClass = "";
        if ("mysql".equals(serverType)) {
            driverClass = "com.mysql.jdbc.Driver";
        } else if ("sql".equals(serverType)) {
            driverClass = "net.sourceforge.jtds.jdbc.Driver";
        } else if ("teradata".equals(serverType)) {
            driverClass = "com.teradata.jdbc.teradriver";
        } else if ("oracle".equals(serverType) || "oracleservice".equals(serverType)) {
            driverClass = "oracle.jdbc.driver.OracleDriver";
        } else if ("db2".equals(serverType)) {
            driverClass = "com.db2.jdbc.driver";
        } else if ("sybase".equals(serverType)) {
            driverClass = "com.sybase.jdbc4.jdbc.sybdriver";
        } else if ("postgresql".equals(serverType)) {
            driverClass = "com.postgresql.jdbc.driver";
        } else if ("as400".equals(serverType) || "as400noport".equals(serverType)) {
            driverClass = "com.ibm.as400.access.AS400JDBCDriver";
        } else {
            LOGGER.error("Server Type not matched: " + serverType);
            System.exit(2);
        }
        return driverClass;
    }

    public Connection checkConnection(InputArgs inputArgs) {

        try {
            Class.forName(getForName(inputArgs.getDatabaseServer().toLowerCase()));
            DriverManager.setLoginTimeout(6000);
            con = DriverManager.getConnection(getConnectionString(inputArgs), inputArgs.getUser(), inputArgs.getPass());
            if (con == null) {
            }
        } catch (SQLException e) {
            LOGGER.error("SQLException:" + e.getMessage());
        } catch (ClassNotFoundException e) {
            LOGGER.error("ClassNotFoundException:" + e.getMessage());
        }
        return con;
    }
}
