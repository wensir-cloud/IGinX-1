package cn.edu.tsinghua.iginx.session.DWF_API;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

/**
 * Entity implementation class for Entity: DataSourceTablesDO
 * 仅为模版实体类，不进行持久化
 */
@ApiModel(description = "数据源探测得到的表结构信息")
@Entity
@Table(name = "PLT_SCN_AllTablesInDataSource")
public class DataSourceTablesDO /*implements Serializable*/ {

    // @ApiModelProperty(value = "数据表oid")
    // @Column(name = "plt_oid", columnDefinition = "varchar(32)")
    // @Id
    // private String oid;

    // @ApiModelProperty(value = "创建时间")
    // @Column(name = "plt_createtime", columnDefinition = "timestamp default now()")
    // private Date createTime = new Date();

    @ApiModelProperty(value = "数据源类型", example = "MYSQL")
    @Column(name = "plt_type", columnDefinition = "varchar")
//    @Enumerated(EnumType.STRING)
    private String dataSourceType;

    // @ApiModelProperty(value = "数据库版本",example = "5.7")
    // @Column(name = "plt_version", columnDefinition = "varchar")
    // private String dataBaseVersion;

    // @ApiModelProperty(value = "数据源oid")
    // @Column(name = "plt_datasource_oid", columnDefinition = "varchar(32)")
    // @Id
    // private String dataSourceOid;

    @ApiModelProperty(value = "数据源名称", example = "mysqltest")
    @Column(name = "plt_datasource_name", columnDefinition = "varchar")
    private String dataSourceName;

    @ApiModelProperty(value = "数据表数据库名称,oracle需填写服务名【建议并支持】/SID", example = "test")
    @Column(name = "plt_database", columnDefinition = "varchar")
    private String databaseName;

    @ApiModelProperty(value = "服务器IP", example = "192.168.30.4")
    @Column(name = "plt_host", columnDefinition = "varchar")
    private String serverIp;

    @ApiModelProperty(value = "服务器端口", example = "3306")
    @Column(name = "plt_port", columnDefinition = "integer")
    private Integer serverPort;

    @ApiModelProperty(value = "数据源用户名", example = "test")
    @Column(name = "plt_user", columnDefinition = "varchar")
    private String userName;

    @ApiModelProperty(value = "数据表所在schema", example = "")
    @Column(name = "plt_table_schema",columnDefinition = "varchar")
    private String schemaName;

    @ApiModelProperty(value = "数据表名称",example = "type_test")
    @Column(name = "plt_table_name", columnDefinition = "varchar")
    private String tableName;

    public DataSourceTablesDO(ResultSet t,String ip,integer port,String dsType){
        this.tableName=t.getString(3);
        this.databaseName=t.getString(1);
        this.schemaName=t.get("TABLE_SCHEM");
        this.userName=null;
        this.serverport=port;
        this.serverIp=ip;
        this.dataSourceName=dsType;
        this.dataSourceType=dsType;
    }

    public String getOid() {
        return oid;
    }
    public void setOid(String oid) {
        this.oid = oid;
    }

    public Date getCreateTime() {
        return createTime;
    }
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getDataSourceType() {
        return dataSourceType;
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
