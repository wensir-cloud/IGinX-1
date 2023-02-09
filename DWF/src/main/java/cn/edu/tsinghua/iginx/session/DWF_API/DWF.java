package cn.edu.tsinghua.iginx.session.DWF_API;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session_v2.write.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iginx.jdbc.*;


import java.sql.SQLException;
import java.util.*;

public class DWF {

    private String addDataSource="ADD STORAGEENGINE (\"%s\",%d,\"%s\",\"%s\");";
    private static Session session;

    private static IginXConnection connection;

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    private static final String showClusterInfo = "SHOW CLUSTER INFO;";

    private static String neededTablePattern="*";

    private static String neededColumnPattern="*";

    private static String neededDatabasePattern="*";

    public DWF(String databasePattern,String tablePattern,String columnPattern){
        this.neededTablePattern=tablePattern;
        this.neededColumnPattern=columnPattern;
        this.neededDatabasePattern=databasePattern;
    }

    /**
     * 增加一个数据源
     * @param host 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @param hasData   数据源数据是否需要被访问
     * @param isReadOnly    数据源是否只读，未来不安排写入数据
     * @return  是否成功
     */
    boolean addDataSource(String host, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly){
        session=new Session("127.0.0.1",6667);
        try {
            session.openSession();
            String addEngine=String.format(addDataSource,host,port,type,"username:"+type+","+"password:"+pass);
            logger.info(addEngine);
            session.executeSql(addEngine);
            return true;
        }catch(SessionException e){
            logger.info("SessionException",e);
            return false;
        }catch(ExecutionException e){
            logger.info("ExecutionException",e);
            return false;
        }
    }

    /**
     * 增加一个数据源，并将其符合模式的表加入系统
     * @param host 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @param hasData   数据源数据是否需要被访问
     * @param isReadOnly    数据源是否只读，未来不安排写入数据
     * @param pattern   表需要符合的模式
     * @return  是否成功
     */
    boolean addTablesFromDataSource(String host, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly, String pattern){
        this.neededTablePattern=pattern;
        try {
            addDataSource(host, port, type, user, pass, hasData, isReadOnly);
            return true;
        }catch (Exception e){
            logger.info("Exception!!!",e);
            return false;
        }
    }

    /**
     * 增加一个数据源，并将其符合模式的列加入系统
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @param hasData   数据源数据是否需要被访问
     * @param isReadOnly    数据源是否只读，未来不安排写入数据
     * @param pattern   列需要符合的模式
     * @return  是否成功
     */
    boolean addColumnsFromDataSource(String ip, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly, String pattern){
        this.neededColumnPattern=pattern;
        try {
            addDataSource(ip, port, type, user, pass, hasData, isReadOnly);
            return true;
        }catch (Exception e){
            logger.info("Exception!!!",e);
            return false;
        }
    }


    List<DataSourceTablesDO> descTableFromDataSource(String ip, int port, String user, String pass, String pattern){
         session=new Session(ip,port,user,pass);
         ArrayList res=new ArrayList<>();
         try {
             session.openSession();
             Properties properties = new Properties();
             properties.setProperty(Config.USER, user);
             properties.setProperty(Config.PASSWORD, pass);
             String url = String.format(Config.IGINX_URL_PREFIX + "%s:%s/", ip, port);
             connection=new IginXConnection(url,properties);
         }catch (SessionException e){
             logger.info("SessionException",e);
             return null;
         } catch (IginxUrlException e) {
             throw new RuntimeException(e);
         } catch (SQLException e) {
             throw new RuntimeException(e);
         }
         return res;
    }
//        Session session = new Session(ip,port,user,pass);
//        session.openSession();
//        List<DataSourceColumnsDO> tables;
//        tables=[];
//        String statement=queryAllTables(type);
//        SessionExecuteSqlResult res = session.executeSql(statement);
//        while(res.next())  {
//            t=res.next();
//            if tableFitPattern(t,pattern){
//                tables.add(DataSourceTablesDO(t,this.ip,this.port,this.type));
//            }
//        }
//        return tables;

    private boolean tableFitPattern(Table t,String pattern){       // fit pattern?
        return true;
    }

    private String queryAllTables(String type){                    //select all tables
//        String s="select *";
//        if type.equals("iotdb"){
//            s="select * from root";
//        }
//        else if type.equals("postgresql"){
//            s="select * from pg_table";
//        }
//        else{
//            System.out.println("engin type do not surport!");
//        }
        return "s";
    }


    List<DataSourceTablesDO> descDBFromDataSource(String ip, int port, String user, String pass, String pattern){
        Session session = new Session(ip,port,user,pass);
        try {
            session.openSession();
        }catch(SessionException e){
            logger.info("SessionException",e);
            return null;

        List<DataSourceTablesDO> DBs;


        }
        return DBs;
    }

     private boolean DBFitPattern(Table t,String pattern){       // fit pattern?
        return true;
    }

    private String queryAllDBs(String type){                    //select all tables
//        String s="select *";
//        if type.equals("iotdb"){
//            s="select * from root";
//        }
//        else if type.equals("postgresql"){
//            s="select datname from pg_database";
//        }
//        else{
//            System.out.println("engin type do not surport!");
//        }
        return "s";
    }


    List<MetaInfo> descDataSource(String ip, int port, String user, String pass){
//        Session session = new Session(ip,port,user,pass);
//        session.openSession();
//        List<DataSourceColumnsDO> tables;
        tables=[];
//        String statement=queryAllTables(type);
//        SessionExecuteSqlResult res = session.executeSql(statement);
//        while(res.next())  {
//            t=res.next();
//
//            tables.add(DataSourceTablesDO(t,this.ip,this.port,this.type));
//
//        }
        return tables;
    }


    /**
     *  移除一个数据源
     * @param ip 数据源网络连接地址
     * @param port  数据源网络连接端口
     * @param type  数据源类型，如Postgres等
     * @param user  用户名
     * @param pass  密码
     * @return 是否成功
     */
    boolean removeDataSource(String ip, int port, String type, String user, String pass, String pattern);


}
