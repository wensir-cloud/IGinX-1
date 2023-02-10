package cn.edu.tsinghua.iginx.session.DWF_API;

import java.sql.SQLException;

public class DataSourceColumnsDO/* implements Serializable */{
    private String dataType;
    private String columnName;
    public DataSourceColumnsDO(String columnName,String dataType) throws SQLException {
        this.dataType=dataType;
        this.columnName=columnName;
        System.out.println("columnName      "+columnName);
        System.out.println("datatype        "+dataType);
    }
    public String getDataType(){return this.dataType;}
    public String getColumnName(){return this.columnName;}

}
