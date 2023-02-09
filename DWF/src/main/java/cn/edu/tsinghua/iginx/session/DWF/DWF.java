package cn.edu.tsinghua.iginx.session.DWF;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import org.apache.commons.lang3.RandomStringUtils;
import cn.edu.tsinghua.iginx.session.Session;

import cn.edu.tsinghua.iginx.session.DWF_API.*;

import java.util.Arrays;
import java.util.List;

public class DWF {

    private static Session session;

    private static final String prefix = "wy";

    private static final String S1 = "s1";
    private static final String S2 = "s2";
    private static final String S3 = "s3";
    private static final String S4 = "s4";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 15000L;

    private static final List<String> funcTypeList = Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");

    private static final String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";

    private static final String delete = "DELETE FROM us.d1.s1 WHERE key > 105 and key < 115;";

    private static final String simpleQuery = "SELECT s1 FROM us.d1 WHERE key > 100 and key < 120;";
    private static final String valueFilterQuery = "SELECT s1 FROM us.d1 WHERE key > 0 and key < 10000 and s1 > 200 and s1 < 210;";
    private static final String limitQuery = "SELECT s1 FROM us.d1 WHERE key > 0 and key < 10000 limit 10;";
    private static final String limitOffsetQuery = "SELECT s1 FROM us.d1 WHERE key > 0 and key < 10000 limit 10 offset 5;";
    private static final String aggregateQuery = "SELECT %s(%s), %s(%s) FROM us.d1 WHERE key > %s and key < %s;";
    private static final String downSample = "SELECT %s(%s), %s(%s) FROM us.d1 GROUP (%s, %s) BY %s;";
    private static final String lastQuery = "SELECT %s(%s), %s(%s) FROM us.d1 WHERE key > %s;";
    private static final String countAll = "SELECT COUNT(*) FROM us.d1;";

    private static final String deleteTimeSeries = "DELETE TIME SERIES us.d1.s2, us.d1.s4;";
    private static final String addStorageEngines = "ADD STORAGEENGINE (\"127.0.0.1\", 6667, \"iotdb11\", \"username: root, password: root\"), (\"127.0.0.1\", 6668, \"influxdb\", \"key: val\");";

    private static final String countPoints = "COUNT POINTS;";
    private static final String showReplication = "SHOW REPLICA NUMBER;";
    private static final String showTimeSeries = "SHOW TIME SERIES;";
    private static final String showClusterInfo = "SHOW CLUSTER INFO;";
    private static final String clearData = "CLEAR DATA;";

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();
        //

        System.out.println("success!!!");

        session.closeSession();
    }
}

