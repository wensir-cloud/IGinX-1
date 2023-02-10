package cn.edu.tsinghua.iginx.session.DWF_API;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iginx.jdbc.*;
import cn.edu.tsinghua.iginx.session.Session;

public class DataSourceDBsDO {
    private long id;
    private int port;
    private String ip;
    private String type;


    public DataSourceDBsDO(long id,String ip,int port,String type) throws SQLException {
        this.ip=ip;
        this.port=port;
        this.id=id;
        this.type=type;
        System.out.println("id ip port type:        "+String.valueOf(id)+ip+String.valueOf(port)+type);
    }
    public String getIp(){return this.ip;}
    public int getPort(){return this.port;}
    public long getId(){return this.id;}
    public String getType(){return this.type;}

}