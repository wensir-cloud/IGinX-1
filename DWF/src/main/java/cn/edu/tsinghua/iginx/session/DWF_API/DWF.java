package cn.edu.tsinghua.iginx.session.DWF_API;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.jdbc.IginXConnection;
import cn.edu.tsinghua.iginx.session.Column;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public class DWF {

    private String i_ip="127.0.0.1";
    private int i_port=6888;
    private String i_user="root";
    private String i_pass="root";

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
    public boolean addDataSource(String host, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly) throws SessionException {
        session=new Session("127.0.0.1",6888);
        Map<String,String>extraParams=new HashMap<>();
        extraParams.put("username",user);
        extraParams.put("password",pass);
        extraParams.put("hasData",String.valueOf(hasData));
        extraParams.put("isReadOnly",String.valueOf(isReadOnly));
        try {
            session.openSession();
//            String addEngine=String.format(addDataSource,host,port,type,"username:"+type+","+"password:"+pass);
//            logger.info(addEngine);
//            session.executeSql(addEngine);
            session.addStorageEngine(host,port,type,extraParams);
            session.closeSession();
            return true;
        }catch(SessionException e){
            logger.info("SessionException",e);
            session.closeSession();
            return false;
        }catch(ExecutionException e){
            logger.info("ExecutionException",e);
            session.closeSession();
            return false;
        }
    }
    public boolean addTablesFromDataSource(String host, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly, String pattern) throws SessionException {
        return addDataSource(host,port,type,user,pass,hasData,isReadOnly);
    }
    public boolean addColumnsFromDataSource(String ip, int port, String type, String user, String pass, boolean hasData, boolean isReadOnly, String pattern) throws SessionException {
        return addDataSource(ip,port,type,user,pass,hasData,isReadOnly);
    }


    public List<DataSourceColumnsDO> descColumnFromDataSource(String ip,int port,String user,String pass,String pattern){
        session = new Session(i_ip, i_port, i_user, i_pass);
        ArrayList res = new ArrayList<>();
        ArrayList paths=new ArrayList<>();
        ArrayList datatypes=new ArrayList<>();
        ArrayList columns=new ArrayList<>();
        ArrayList tables=new ArrayList<>();
        ArrayList databases=new ArrayList<>();

        try {
            session.openSession();
            List<Column> columnList = session.showColumns();
            columnList.forEach(column->{
                String path=column.getPath();
                String datatype=column.getDataType().toString();
                paths.add(column.getPath());
                datatypes.add(column.getDataType().toString());
                String[] p =path.split("\\.");
                if(pattern.equals("*")){
                    columns.add(p[p.length-1]);
                    tables.add(p[p.length-2]);
                    if((p.length)-3>=0){
                        databases.add(p[p.length-3]);
                    }
                    else{
                        databases.add("null");
                    }
                }
                else {
                    if (path.startsWith(pattern)) {
                        columns.add(p[p.length - 1]);
                        tables.add(p[p.length - 2]);
                        if ((p.length) - 3 >= 0) {
                            databases.add(p[p.length - 3]);
                        } else {
                            databases.add("null");
                        }
                    }
                }
                try {
                    res.add(new DataSourceColumnsDO(p[p.length-1],datatype));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            return res;

        } catch (SessionException e) {
            logger.info("SessionException", e);
            return null;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    public List<DataSourceTablesDO> descTableFromDataSource(String ip, int port, String user, String pass, String pattern) throws SessionException, ExecutionException {
        session = new Session(i_ip, i_port, i_user, i_pass);
        ArrayList res = new ArrayList<>();
        ArrayList paths=new ArrayList<>();
        ArrayList datatypes=new ArrayList<>();
        ArrayList columns=new ArrayList<>();
        ArrayList tables=new ArrayList<>();
        ArrayList databases=new ArrayList<>();
        try {
            session.openSession();
            List<Column> columnList = session.showColumns();
            columnList.forEach(column->{
                paths.add(column.getPath());
                datatypes.add(column.getDataType().toString());
                String path=column.getPath();
                String datatype=column.getDataType().toString();
            String[] p =path.split("\\.");
            if(pattern.equals("*")){
                columns.add(p[p.length-1]);
                tables.add(p[p.length-2]);
                if((p.length)-3>=0){
                    databases.add(p[p.length-3]);
                }
                else{
                    databases.add("null");
                }
            }
            else {
                if (path.startsWith(pattern)) {
                    columns.add(p[p.length - 1]);
                    tables.add(p[p.length - 2]);
                    if ((p.length) - 3 >= 0) {
                        databases.add(p[p.length - 3]);
                    } else {
                        databases.add("null");
                    }
                }
            }
        });
            HashSet h=new HashSet(tables);
            tables.clear();
            tables.addAll(h);
            tables.forEach(t->{
                try {
                    res.add(new DataSourceTablesDO((String) t));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            return res;

        } catch (SessionException e) {
            logger.info("SessionException", e);
            return null;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    public List<DataSourceDBsDO> descDBFromDataSource(String ip, int port, String user, String pass, String pattern) {
        session = new Session(i_ip, i_port, i_user, i_pass);
        ArrayList res = new ArrayList<>();
        ArrayList ids=new ArrayList<>();
        ArrayList ips=new ArrayList<>();
        ArrayList types=new ArrayList<>();
        ArrayList ports=new ArrayList<>();
        try {
            session.openSession();
            List<StorageEngineInfo> storageEngineInfoList = session.getClusterInfo().getStorageEngineInfos();
            storageEngineInfoList.forEach(s-> {
                if (pattern.equals("*")) {
                    long d = s.id;
                    int po = s.port;
                    String p = s.ip;
                    String t = s.type;
                    try {
                        res.add(new DataSourceDBsDO(d, p, po, t));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    if (s.type.startsWith(pattern)) {
                        long d = s.id;
                        int po = s.port;
                        String p = s.ip;
                        String t = s.type;
                        try {
                            res.add(new DataSourceDBsDO(d, p, po, t));
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }});
            return res;
        }catch (SessionException e) {
            logger.info("SessionException", e);
            return null;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<DataSourceTablesDO> descAllTableDataSource(String ip, int port, String user, String pass) throws SessionException, ExecutionException {
        String pattern="*";
        return descTableFromDataSource(ip,port,user,pass,pattern);
    }

    public List<DataSourceColumnsDO> descAllColumnDataSource(String ip, int port, String user, String pass) throws SessionException, ExecutionException {
        String pattern="*";
        return descColumnFromDataSource(ip,port,user,pass,pattern);
    }

    public List<DataSourceDBsDO> descAllDBDataSource(String ip, int port, String user, String pass) throws SessionException, ExecutionException {
        String pattern="*";
        return descDBFromDataSource(ip,port,user,pass,pattern);
    }
    public boolean removeDataSource(long id) {
        session=new Session(i_ip,i_port,i_user,i_pass);
        try {
            session.removeHistoryDataSource(id);
            return true;
        }catch (Exception e){
            return false;
        }
    }


}
