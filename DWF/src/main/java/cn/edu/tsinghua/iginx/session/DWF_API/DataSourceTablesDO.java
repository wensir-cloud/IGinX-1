package cn.edu.tsinghua.iginx.session.DWF_API;

import java.sql.SQLException;
public class DataSourceTablesDO {
    private String tableName;
    public DataSourceTablesDO(String tableName) throws SQLException {
        this.tableName=tableName;
        System.out.println("tableName       "+tableName);
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
