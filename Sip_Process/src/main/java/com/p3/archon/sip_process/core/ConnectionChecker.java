package com.p3.archon.sip_process.core;

import com.p3.archon.sip_process.bean.InputArgs;
import lombok.SneakyThrows;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;

import static com.p3.archon.sip_process.constants.DatabaseConstants.*;

/**
 * Created by Suriyanarayanan K
 * on 29/02/20 12:48 PM.
 */
public class ConnectionChecker {

    Connection con;
    private Logger LOGGER = Logger.getLogger(this.getClass().getName());

    public String getConnectionString(InputArgs inputArgs) {
        Object[] replaceValues = new Object[]{inputArgs.getHostName(), inputArgs.getPortNo(), inputArgs.getDatabaseName()};
        return MessageFormat.format(getConnectionURL(inputArgs.getDatabaseServer().toLowerCase()), replaceValues);
    }

    public String getConnectionURL(String type) {
        switch (type) {
            case TERADATA:
                return "jdbc:teradata://{0}/{2}";
            case SQLWINAUTH:
                return "jdbc:jtds:sqlserver://localhost/{2};appName=SchemaCrawler;useCursors=true;useNTLMv2=true;domain={0};";
            case SQL:
                return "jdbc:jtds:sqlserver://{0}:{1}/{2};appName=SchemaCrawler;useCursors=true;";
            case ORACLE:
                return "jdbc:oracle:thin:@{0}:{1}:{2}";
            case ORACLESERVICE:
                return "jdbc:oracle:thin:@//{0}:{1}/{2}";
            case MYSQL:
                return "jdbc:mysql://{0}:{1}/{2}?nullNamePatternMatchesAll=true&logger=Jdk14Logger&dumpQueriesOnException=true&dumpMetadataOnColumnNotFound=true&maxQuerySizeToLog=4096&useSSL=false";
            case DB2:
                return "jdbc:db2://{0}:{1}/{2};retrieveMessagesFromServerOnGetMessage=true;";
            case SYBASE:
                return "jdbc:jtds:sybase://{0}:{1}/{2}";
            case POSTGRESQL:
                return "jdbc:postgresql://{0}:{1}/{2}?ApplicationName=SchemaCrawler";
            case AS400:
                return "jdbc:as400://{0}:{1}/{2};translate hex=character;translate binary=true";
            case AS400NOPORT:
                return "jdbc:as400://{0}/{2};translate hex=character;translate binary=true";
        }
        return null;
    }

    public String getForName(String serverType) {
        String driverClass = "";
        switch (serverType) {
            case MYSQL:
                driverClass = MYSQL_DRIVER;
                break;
            case SQL:
                driverClass = SQL_DRIVER;
                break;
            case TERADATA:
                driverClass = TERADATA_DRIVER;
                break;
            case ORACLE:
            case ORACLESERVICE:
                driverClass = ORACLE_DRIVER;
                break;
            case DB2:
                driverClass = DB2_DRIVER;
                break;
            case SYBASE:
                driverClass = SYBASE_DRIVER;
                break;
            case POSTGRESQL:
                driverClass = POSTGRESQL_DRIVER;
                break;
            case AS400:
            case AS400NOPORT:
                driverClass = AS400_DRIVER;
                break;
            default:
                LOGGER.error("Server Type not matched: " + serverType);
                System.exit(2);
        }
        return driverClass;
    }

    @SneakyThrows
    public Connection checkConnection(InputArgs inputArgs) {

        try {
            Class.forName(getForName(inputArgs.getDatabaseServer().toLowerCase()));
            DriverManager.setLoginTimeout(6000);
            con = DriverManager.getConnection(getConnectionString(inputArgs), inputArgs.getUser(), inputArgs.getPass());

        } catch (SQLException e) {
            LOGGER.error("SQLException:" + e.getMessage());
        } catch (ClassNotFoundException e) {
            LOGGER.error("ClassNotFoundException:" + e.getMessage());
        }
        if (con == null) {
            throw new Exception("Connection not established");
        }
        return con;
    }
}
