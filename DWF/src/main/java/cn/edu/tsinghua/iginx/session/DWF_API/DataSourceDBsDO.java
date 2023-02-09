package cn.edu.tsinghua.iginx.session.DWF_API;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.persistence.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iginx.jdbc.*;
import cn.edu.tsinghua.iginx.session.Session;

public class DataSourceDBsDO {
    private IginXConnection connection;
    private Session session;

    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private DatabaseMetaData dbMetaData;
    private String dataSourceType;

    public DataSourceDBsDO(Session session,IginXConnection connection){
        this.connection=connection;
        this.session=session;
        this.dbMetaData=new IginXDatabaseMetadata(connection, session);
    }

    public Connection getConnection() {
        return connection;
    }
    public String getDataSourceType() {

        try {
            return dbMetaData.getDatabaseProductName();
        }catch(SQLException e){
            logger.info("SQLException",e);
            return "Some Error!!!";
        }
    }
    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getDataBaseVersion() {
        return dataBaseVersion;
    }
    public void setDataBaseVersion(String dataBaseVersion) {
        this.dataBaseVersion = dataBaseVersion;
    }

    public String getDataSourceOid() {
        return dataSourceOid;
    }
    public void setDataSourceOid(String dataSourceOid) {
        this.dataSourceOid = dataSourceOid;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }
    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public String getDatabaseName() {
        return databaseName;
    }
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getServerIp() {
        return serverIp;
    }
    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public Integer getServerPort() {
        return serverPort;
    }
    public void setServerPort(Integer serverPort) {
        this.serverPort = serverPort;
    }

    public String getUserName() {
        return userName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getSchemaName() {
        return schemaName;
    }
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
