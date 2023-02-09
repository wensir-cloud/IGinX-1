package edu.thss.platform.dao.dwf3s;

import com.datastax.driver.core.Cluster;
import com.mongodb.MongoException;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import edu.thss.platform.domain.dwf3s.ColumnSchema;
import edu.thss.platform.domain.dwf3s.DataSchema;
import edu.thss.platform.domain.modeler.MetaAttributeDO;
import edu.thss.platform.domain.modeler.MetaDataSourceDO;
import edu.thss.platform.domain.modeler.valuetype.AttributeValueType;
import edu.thss.platform.exception.PlatformException;
import org.bson.conversions.Bson;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import redis.clients.jedis.Jedis;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lta
 */
public abstract class DataSourceUtils {

  static Logger logger = LoggerFactory.getLogger(DataSourceUtils.class);

  /**
   * Get primary keys
   *
   * @param dataSchema 数据模式信息
   */
  public static String getPrimaryKeyName(DataSchema dataSchema) { // 目前只处理1个主键的情况
    for (ColumnSchema columnSchema : dataSchema.getColumnSchemaList()) {
      if (columnSchema.isPrimaryKey()) return columnSchema.getColumnName();
    }
    return null;
  }

  public static AttributeValueType getPrimaryKeyType(DataSchema dataSchema, String primaryKey) { // 目前只处理1个主键的情况
    for (ColumnSchema columnSchema : dataSchema.getColumnSchemaList()) {
      if (primaryKey.equals(columnSchema.getColumnName())) return columnSchema.getAttributeValueType();
    }
    return null;
  }

  public static MetaAttributeDO getOriPrimaryKey(DataSchema dataSchema) { // 目前只处理1个主键的情况
    for (ColumnSchema columnSchema : dataSchema.getColumnSchemaList()) {
      if (columnSchema.isPrimaryKey()) return convertToMetaAttribute(columnSchema);
    }
    return null;
  }

  public static MetaAttributeDO getBandedPrimaryKey(DataSchema dataSchema, String primaryKey) { // 目前只处理1个主键的情况
    for (ColumnSchema columnSchema : dataSchema.getColumnSchemaList()) {
      if (columnSchema.getColumnName().equals(primaryKey)) return convertToMetaAttribute(columnSchema);
    }
    return null;
  }

  public static List<MetaAttributeDO> getOriBandedPrimaryKey(DataSchema dataSchema, String bandedPrimaryKey) { // 目前只处理1个主键的情况, 0绑定的主键, 1原本的主键
    List<MetaAttributeDO> res = new ArrayList<>();
    List<ColumnSchema> columnSchemaList = dataSchema.getColumnSchemaList();
    for (ColumnSchema columnSchema : columnSchemaList) {
      if (columnSchema.getColumnName().equals(bandedPrimaryKey)) { res.add(convertToMetaAttribute(columnSchema)); break; }
    }
    for (ColumnSchema columnSchema : columnSchemaList) {
      if (columnSchema.isPrimaryKey()) { res.add(convertToMetaAttribute(columnSchema)); break; }
    }
    return res;
  }

  private static MetaAttributeDO convertToMetaAttribute(ColumnSchema columnSchema) {
    MetaAttributeDO metaAttributeDO = new MetaAttributeDO();
    metaAttributeDO.setAttributeName(columnSchema.getColumnName());
    metaAttributeDO.setId(columnSchema.getColumnName()); // ID设置为与attributeName相同
    metaAttributeDO.setValueType(columnSchema.getAttributeValueType().toString());
    metaAttributeDO.setNullable(columnSchema.isNull());
    metaAttributeDO.setIsPrimaryKey(columnSchema.isPrimaryKey());
    metaAttributeDO.setIsIdentity(columnSchema.isIdentity());
    if ((columnSchema.getAttributeValueType().equals(AttributeValueType.Date) || columnSchema.getAttributeValueType().equals(AttributeValueType.TimeStamp))
            && ("now()".equals(columnSchema.getDefaultValue()) || "statement_timestamp()".equals(columnSchema.getDefaultValue())))
      metaAttributeDO.setDefaultValue("当前时间"); // 返回成前端指定处理的字段
    else metaAttributeDO.setDefaultValue(columnSchema.getDefaultValue());
    return metaAttributeDO;
  }

  public static Connection getDataSourceConnection(MetaDataSourceDO dataSource) {
    try {
      Class.forName("org.postgresql.Driver");
      String pgUrl = String.format("jdbc:postgresql://%s:%s/%s", dataSource.getServerIp(), dataSource.getServerPort(), dataSource.getDataBaseName());
      logger.debug("org.postgresql.Driver " + pgUrl);
      return DriverManager.getConnection(pgUrl, dataSource.getUserName(), dataSource.getPassword());
    } catch (Exception e) {
      String errorMsg = String.format("数据源'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage());
      logger.error(errorMsg);
      throw new PlatformException(errorMsg);
    }
  }
  public static Connection getDataSourceConnection(MetaDataSourceDO dataSource, String schema) {
    Connection connection = getDataSourceConnectionWithNewSchema(dataSource, schema);
    if (connection == null) connection = getDataSourceConnectionWithOldSchema(dataSource, schema);
    if (connection == null) {
      String errorMsg = String.format("数据源'%s'连接错误：schema为%s时无法建立连接", dataSource.getDataSourceName(), schema);
      logger.error(errorMsg);
      throw new PlatformException(errorMsg);
    }
    return connection;
  }

  public static Connection getDataSourceConnectionWithOldSchema(MetaDataSourceDO dataSource, String schema) {
    try {
      Class.forName("org.postgresql.Driver");
      String pgUrl = String.format("jdbc:postgresql://%s:%s/%s?searchpath=\"%s\"", dataSource.getServerIp(), dataSource.getServerPort(), dataSource.getDataBaseName(), schema);
      logger.debug("org.postgresql.Driver " + pgUrl);
      return DriverManager.getConnection(pgUrl, dataSource.getUserName(), dataSource.getPassword());
    } catch (Exception e) {
      logger.error(String.format("数据源'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage()));
      return null;
    }
  }

  public static Connection getDataSourceConnectionWithNewSchema(MetaDataSourceDO dataSource, String schema) {
    try {
      Class.forName("org.postgresql.Driver");
      String pgUrl = String.format("jdbc:postgresql://%s:%s/%s?currentSchema=\"%s\"", dataSource.getServerIp(), dataSource.getServerPort(), dataSource.getDataBaseName(), schema);
      logger.debug("org.postgresql.Driver " + pgUrl);
      return DriverManager.getConnection(pgUrl, dataSource.getUserName(), dataSource.getPassword());
    } catch (Exception e) {
      logger.error(String.format("数据源'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage()));
      return null;
    }
  }

  public static Connection getIotdbConnection(MetaDataSourceDO metaDataSourceDO) {
    try {
      if (getIotdbVersion(metaDataSourceDO.getDataBaseVersion()) < 11.0)
          throw new PlatformException("当前系统只支持0.11.x及其以上版本的iotdb");

      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      String iotdbUrl = String.format("jdbc:iotdb://%s:%s/", metaDataSourceDO.getServerIp(), metaDataSourceDO.getServerPort());
      logger.debug("org.apache.iotdb.jdbc.IoTDBDriver " + iotdbUrl);
      return DriverManager.getConnection(iotdbUrl, metaDataSourceDO.getUserName(), metaDataSourceDO.getPassword());
//            connection.setNetworkTimeout(Executors.newFixedThreadPool(1), 1000);
//            Properties props = new Properties();
//            props.setProperty("user", metaDataSourceDO.getUserName());
//            props.setProperty("password", metaDataSourceDO.getPassword());
//            props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT, "2000");
    } catch (Exception e) {
      String errorMsg = String.format("iotdb数据源'%s'连接错误（%s）", metaDataSourceDO.getDataSourceName(), e.getMessage());
      logger.error(errorMsg);
      throw new PlatformException(errorMsg);
    }
  }

  public static Connection getMysqlConnection(MetaDataSourceDO dataSource) {
    try {
      Class.forName("com.mysql.jdbc.Driver"); // &autoReconnect=true&failOverReadOnly=false&maxReconnects=10
      String mysqlUrl = String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC&useSSL=false", dataSource.getServerIp(), dataSource.getServerPort(), dataSource.getDataBaseName());
      logger.debug("com.mysql.jdbc.Driver " + mysqlUrl);
      return DriverManager.getConnection(mysqlUrl, dataSource.getUserName(), dataSource.getPassword());
    } catch (Exception e) {
      String errorMsg = String.format("mysql数据源'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage());
      logger.error(errorMsg);
      throw new PlatformException(errorMsg);
    }
  }

  public static Connection getOracleConnection(MetaDataSourceDO dataSource) {
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      String oracleUrl = String.format("jdbc:oracle:thin:@//%s:%s/%s", dataSource.getServerIp(), dataSource.getServerPort(), dataSource.getDataBaseName());
      logger.debug("oracle.jdbc.driver.OracleDriver " + oracleUrl);
      return DriverManager.getConnection(oracleUrl, dataSource.getUserName(), dataSource.getPassword());
    } catch (Exception e) {
      String errorMsg = String.format("oracle数据源'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage());
      logger.error(errorMsg);
      throw new PlatformException(errorMsg);
    }
  }

  public static Connection getSqlServerConnection(MetaDataSourceDO dataSource) {
    try {
//      Class.forName("com.microsoft.jdbc.sqlserver.SQLServerDriver");
      String mssqlUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;user=%s;password=%s;loginTimeout=5;", dataSource.getServerIp(), dataSource.getServerPort(), dataSource.getDataBaseName(), dataSource.getUserName(), dataSource.getPassword());
      logger.debug("com.microsoft.jdbc.sqlserver.SQLServerDriver " + mssqlUrl);
      DriverManager.setLoginTimeout(5);
      return DriverManager.getConnection(mssqlUrl);
    } catch (Exception e) {
      String errorMsg = String.format("sqlserver数据源'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage());
      logger.error(errorMsg);
      throw new PlatformException(errorMsg);
    }
  }

  public static Boolean getMongodbConnection(MetaDataSourceDO dataSource) {
    String mongodbUrl = String.format("mongodb://%s:%s@%s:%s/?authSource=%s&serverSelectionTimeoutMS=3000", dataSource.getUserName(), dataSource.getPassword(),dataSource.getServerIp(), dataSource.getServerPort(), dataSource.getDataBaseName());
    logger.debug("getMongodbConnection" + mongodbUrl);
    try (MongoClient mongoClient = MongoClients.create(mongodbUrl)) {
      MongoDatabase database = mongoClient.getDatabase( dataSource.getDataBaseName());
      try {
        Bson command = new BsonDocument("ping", new BsonInt64(1));
        Document commandResult = database.runCommand(command);
        return true;
      } catch (MongoException me) {
        String errorMsg = String.format("Mongodb数据源'%s'连接错误（%s）", dataSource.getDataSourceName(), me.getMessage());
        logger.error("getMongodbConnection" + errorMsg);
        return false;
      } finally {
        mongoClient.close();
      }
    }
  }

  public static Boolean getRedisConnection(MetaDataSourceDO dataSource) {
    try {
      Jedis jedis = new Jedis(dataSource.getServerIp(),dataSource.getServerPort());
      jedis.auth(dataSource.getPassword());
      jedis.ping();
      return true;
    } catch (Exception e){
      String errorMsg = String.format("Redis'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage());
      logger.error("getRedisConnection" + errorMsg);
      return false;
    }
  }

  public static Boolean getNeo4jConnection(MetaDataSourceDO dataSource) {
    Driver driver = null;
    Session session = null;
    try {
      String neo4jUrl = String.format("bolt://%s:%s", dataSource.getServerIp(), dataSource.getServerPort());
      logger.debug("getNeo4jConnection" + neo4jUrl);
      driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic(dataSource.getUserName(), dataSource.getPassword()));
      session = driver.session();
      session.run("MATCH (n) RETURN distinct labels(n)");
      session.close();
      driver.close();
      return true;
    } catch (Exception e){
      if (session != null){ session.close(); }
      if (driver != null){ driver.close(); }
      String errorMsg = String.format("Neo4j'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage());
      logger.error("getNeo4jConnection" + errorMsg);
      return false;
    }
  }

  public static Boolean getCassandraConnection(MetaDataSourceDO dataSource){
    Cluster cluster = null;
    try {
      Class.forName("com.datastax.driver.core.Session");
      cluster = Cluster.builder().addContactPoints(dataSource.getServerIp())
                                  .withPort(dataSource.getServerPort())
                                  .withCredentials(dataSource.getUserName(), dataSource.getPassword()).build();
      cluster.connect();
      cluster.close();
      return true;
    } catch (Exception e){
      if (cluster != null){ cluster.close(); }
      String errorMsg = String.format("Cassandra'%s'连接错误（%s）", dataSource.getDataSourceName(), e.getMessage());
      logger.error("getCassandraConnection" + errorMsg);
      return false;
    }
  }

  public static void closeConnection(Connection connection) {
    try { connection.close(); } catch (Exception ignored) { }
  }

  public static Double getIotdbVersion(String dataBaseVersion) {
    try {
      dataBaseVersion = dataBaseVersion.substring("0.".length());
      return Double.parseDouble(dataBaseVersion);
    } catch (Exception e) { return 9.0; }
  }

  public static ResultSet getResultSetBySql(Statement statement, String sql) {
    return getResultSetBySql(statement, sql, null, null);
  }
  public static ResultSet getResultSetBySql(Statement statement, String sql, Integer rowsNumber, Integer startIndex) {
    try {
      String subQuerySql = "";
      if (rowsNumber != null && rowsNumber >= 0) subQuerySql += " limit " + rowsNumber; // statement.setMaxRows(rowsNumber);
      if (startIndex != null && startIndex > 0) subQuerySql += " offset " + startIndex;
      subQuerySql = String.format("select * from (%s) subquery %s", sql, subQuerySql);
      statement.executeQuery(subQuerySql);
      return statement.getResultSet();
    } catch (Exception e) { throw new PlatformException(String.format("数据源查询错误（%s）", sql)); }
  }
  public static String  getSqlWithRowsNumAndStartIndexBySql( String sql, Integer rowsNumber, Integer startIndex) {
    try {
      String subQuerySql = "";
      if (rowsNumber != null && rowsNumber >= 0) subQuerySql += " limit " + rowsNumber; // statement.setMaxRows(rowsNumber);
      if (startIndex != null && startIndex > 0) subQuerySql += " offset " + startIndex;
      subQuerySql = String.format("select * from (%s) subquery %s", sql, subQuerySql);
      return subQuerySql;
    } catch (Exception e) { throw new PlatformException(String.format("数据源查询错误（%s）", sql)); }
  }
  public static Integer getCountBySql(Statement statement, String sql) {
    return getCountBySql(statement, sql, null, null);
  }
  public static Integer getCountBySql(Statement statement, String sql, Integer rowsNumber, Integer startIndex) {
    try {
      String subQuerySql = "";
      if (rowsNumber != null && rowsNumber >= 0) subQuerySql += " limit " + rowsNumber;
      if (startIndex != null && startIndex > 0) subQuerySql += " offset " + startIndex;
      if ("".equals(subQuerySql)) subQuerySql = sql;
      else subQuerySql = String.format("select * from (%s) subquery %s", sql, subQuerySql);

      String countSql = String.format("select count(*) from (%s) subquery", subQuerySql);
      statement.executeQuery(countSql);
      ResultSet rs = statement.getResultSet();
      int count = 0;
      if (rs.next()) { count = rs.getInt(1); }
      return count;
    } catch (Exception e) { throw new PlatformException(String.format("数据源查询错误（%s）", sql)); }
  }


here


  /**
   * Test Util
   */
  public static void show(List<Map<String, Object>> dataSet) {
    if (dataSet == null || dataSet.size() == 0) {
      return;
    }
    for (String column : dataSet.get(0).keySet()) {
      System.out.print(column + "\t|");
    }
    System.out.println();
    for (Map<String, Object> row : dataSet) {
      for (Object data : row.values()) {
        System.out.print(data + "\t|");
      }
      System.out.println();
    }
  }


  public static Map<String, ColumnSchema> getNameMapSchemaObject(List<ColumnSchema> columnSchemas) {
    Map<String, ColumnSchema> columnSchemaMap = new HashMap<>();
    for (ColumnSchema columnSchema : columnSchemas) {
      columnSchemaMap.put(columnSchema.getColumnName(), columnSchema);
    }
    return columnSchemaMap;
  }

  public static void outputResultSet(ResultSet resultSet, PrintStream out) throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    for (int i = 0; i < columnCount; i++) {
      out.print(metaData.getColumnLabel(i + 1) + " ");
    }
    out.println();
    while (resultSet.next()) {
      for (int i = 1; ; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
    out.println("--------------------------");
  }

  /**
   * Convert table name, add schema before table name
   */
  public static String convertTableName(String sql, String schemaName) {
    String subSql = sql.substring(sql.indexOf("from") + 4).trim();
    String[] tableNames = subSql
            .substring(0, subSql.indexOf("where") == -1 ? subSql.length() : subSql.indexOf("where"))
            .trim().split(",");
    for (String tableName : tableNames) {
      sql = sql
              .replaceAll(tableName, schemaName + ".`" + tableName.split("\\s+")[0] + "`" + tableName
                      .replace(tableName.split("\\s+")[0], ""));
    }
    return sql;
  }

  /**
   * 获取相应数据类型的URL
   *
   * @param ip 数据源IP
   * @param port 数据源端口
   * @param databaseName 数据库名
   * @return JDBC URL
   */
  public abstract String getConnectionUrl(String ip, int port,
                                          String databaseName) throws ClassNotFoundException;

  /**
   * 根据数据库实例返回Schema名
   *
   * @param databaseInstance 数据库实例
   */
  public abstract String getSchemaName(String databaseInstance);

  /**
   * 根据数据库实例返回Database Name
   *
   * @param databaseInstance 数据库实例
   */
  public abstract String getDataBaseName(String databaseInstance);


  public static CalciteConnection getCalciteConnection()
      throws SQLException, ClassNotFoundException {
    /** Calcite **/
    // 解决外部实体类中文查询的问题
    System.setProperty("calcite.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    System.setProperty("calcite.default.nationalcharset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    System.setProperty("calcite.default.collation.name", ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new ClassNotFoundException("找不到calcite驱动", e);
    }
    Properties info = new Properties();
    info.setProperty("calcite.default.charset", "UTF-8");
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    return connection.unwrap(CalciteConnection.class);
  }

  public static Schema getCalciteSchema(MetaDataSourceDO dataSource, CalciteConnection calciteConnection)
      throws ClassNotFoundException, SQLException {
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    switch (dataSource.getDataSourceType()) {
      case HDFS:
        try {
          SchemaFactory hdfsCsvMulSchemaFactory = (SchemaFactory) Class
              .forName("org.apache.tianyu.HdfsCsvMulSchemaFactory").newInstance();
          Map<String, Object> operand = new HashMap<>();
          operand.put("host", dataSource.getServerIp());
          operand.put("port", dataSource.getServerPort());
          operand.put("directory",
              dataSource.getDataBaseName().endsWith("/") ? dataSource.getDataBaseName()
                  : dataSource.getDataBaseName().concat("/"));
          operand.put("userName", dataSource.getUserName());
          operand.put("password", dataSource.getPassword());
          Schema hdfsCsvMulSchema = hdfsCsvMulSchemaFactory.create(rootSchema, "hr", operand);
          return hdfsCsvMulSchema;
        } catch (Exception e) {
          throw new ClassNotFoundException(
              String.format(
                  "Can't find class org.apache.tianyu.HdfsCsvMulSchemaFactory for ResultSet"),
              e);
        }
      case HDFS_FS:
        try {
          SchemaFactory hdfsCsvMulSchemaFactory = (SchemaFactory) Class
              .forName("org.apache.tianyu.HdfsFsSchemaFactory").newInstance();
          Map<String, Object> operand = new HashMap<>();
          operand.put("host", dataSource.getServerIp());
          operand.put("port", dataSource.getServerPort());
          operand.put("directory",
              dataSource.getDataBaseName().endsWith("/") ? dataSource.getDataBaseName()
                  : dataSource.getDataBaseName().concat("/"));
          operand.put("userName", dataSource.getUserName());
          operand.put("password", dataSource.getPassword());
          if (dataSource.getFolderDepth() == null) dataSource.setFolderDepth("0");
          operand.put("folderDepth", Integer.parseInt(dataSource.getFolderDepth()));
          Schema hdfsFcSchema = hdfsCsvMulSchemaFactory.create(rootSchema, "hr", operand);
          return hdfsFcSchema;
        } catch (Exception e) {
          throw new ClassNotFoundException(
              String.format(
                  "Can't find class org.apache.tianyu.HdfsFsSchemaFactory for ResultSet"),
              e);
        }
      case HDFS_CSV_DIR:
        try {
          SchemaFactory hdfsBlockCsvSchemaFactory = (SchemaFactory) Class
              .forName("org.apache.tianyu.HdfsBlockCsvSchemaFactory").newInstance();
          Map<String, Object> operand = new HashMap<>();
          operand.put("host", dataSource.getServerIp());
          operand.put("port", dataSource.getServerPort());
          operand.put("directory",
              dataSource.getDataBaseName().endsWith("/") ? dataSource.getDataBaseName()
                  : dataSource.getDataBaseName().concat("/"));
          operand.put("userName", dataSource.getUserName());
          operand.put("password", dataSource.getPassword());
          Schema hdfsCsvDirSchema = hdfsBlockCsvSchemaFactory.create(rootSchema, "hr", operand);
          return hdfsCsvDirSchema;
        } catch (Exception e) {
          throw new ClassNotFoundException(
              String.format(
                  "Can't find class org.apache.tianyu.HdfsBlockCsvSchemaFactory for ResultSet"),
              e);
        }
//      case LOCAL_FILE:
//        try {
//          SchemaFactory LocalBlockCsvSchemaFactory = (SchemaFactory) Class
//              .forName("org.apache.tianyu.LocalCsvMulSchemaFactory").newInstance();
//          Map<String, Object> operand = new HashMap<>();
//          operand.put("host", dataSource.getServerIp());
//          operand.put("port", dataSource.getServerPort());
//          operand.put("directory",
//              dataSource.getDataBaseName().endsWith("/") ? dataSource.getDataBaseName()
//                  : dataSource.getDataBaseName().concat("/"));
//          operand.put("userName", dataSource.getUserName());
//          operand.put("password", dataSource.getPassword());
//          Schema LocalCsvSchema = LocalBlockCsvSchemaFactory.create(rootSchema, "hr", operand);
//          return LocalCsvSchema;
//        } catch (Exception e) {
//          throw new ClassNotFoundException(
//              String.format(
//                  "Can't find class org.apache.tianyu.LocalCsvMulSchemaFactory for ResultSet"),
//              e);
//        }
//      case LOCAL_FS:
//        try {
//          SchemaFactory LocalCsvMulSchemaFactory = (SchemaFactory) Class
//              .forName("org.apache.tianyu.LocalFsSchemaFactory").newInstance();
//          Map<String, Object> operand = new HashMap<>();
//          operand.put("host", dataSource.getServerIp());
//          operand.put("port", dataSource.getServerPort());
//          operand.put("directory",
//              dataSource.getDataBaseName().endsWith("/") ? dataSource.getDataBaseName()
//                  : dataSource.getDataBaseName().concat("/"));
//          operand.put("userName", dataSource.getUserName());
//          operand.put("password", dataSource.getPassword());
//          if (dataSource.getFolderDepth() == null) dataSource.setFolderDepth("0");
//          operand.put("folderDepth", Integer.parseInt(dataSource.getFolderDepth()));
//          Schema hdfsFcSchema = LocalCsvMulSchemaFactory.create(rootSchema, "hr", operand);
//          return hdfsFcSchema;
//        } catch (Exception e) {
//          throw new ClassNotFoundException(
//              String.format(
//                  "Can't find class org.apache.tianyu.LocalFsSchemaFactory for ResultSet"),
//              e);
//        }
      case IOTDB:
        try {
          SchemaFactory iotdbSchemaFactory = (SchemaFactory) Class
              .forName("org.apache.iotdb.calcite.IoTDBSchemaFactory").newInstance();
          Map<String, Object> operand = new HashMap<>();
          operand.put("host", dataSource.getServerIp());
          operand.put("port", dataSource.getServerPort());
          operand.put("userName", dataSource.getUserName());
          operand.put("password", dataSource.getPassword());
          operand.put("flavor", "profil");
          Schema iotdbSchema = iotdbSchemaFactory.create(rootSchema, "hr", operand);
          return iotdbSchema;
        } catch (Exception e) {
          throw new ClassNotFoundException(
              String.format(
                  "Can't find class org.apache.iotdb.calcite.IoTDBSchemaFactory for ResultSet"),
              e);
        }
    }
    System.out.println("不支持的calciteSchema");
    return null;
  }


  /**
   * 类型转换，从Calcite类型到DWF类型
   *
   * @param calciteType Calcite类型
   * @return DWF类型
   */
  public static ColumnType typeConvert(String calciteType) {
    if (calciteType != null) {
      String template = calciteType.split("\\s+")[0].split("\\(")[0];
      switch (template) {
        case "BIGINT":
          return ColumnType.LONG;
        case "BINARY":
          return ColumnType.STRING;
        case "BOOLEAN":
          return ColumnType.BOOLEAN;
        case "ANY":
          return ColumnType.BYTES;
        case "CHAR":
          return ColumnType.STRING;
        case "DATE":
          return ColumnType.DATE;
        case "TIMESTAMP":
          return ColumnType.TIMESTAMP;
        case "DECIMAL":
          return ColumnType.FLOAT;
        case "DOUBLE":
          return ColumnType.FLOAT;
        case "REAL":
          return ColumnType.FLOAT;
        case "INTEGER":
          return ColumnType.INT;
        case "SMALLINT":
          return ColumnType.INT;
        case "TIME":
          return ColumnType.STRING;
        case "TINYINT":
          return ColumnType.INT;
        case "VARCHAR":
          return ColumnType.STRING;
      }
    }
    return ColumnType.STRING;
  }

  /**
   * 类型转换，从DWF类型到Calcite类型
   *
   * @param columnType DWF类型
   * @return Calcite类型
   */
  public static String typeConvert(ColumnType columnType) {
    if (columnType != null) {
      switch (columnType) {
        case INT:
          return "INTEGER";
        case FLOAT:
          return "DOUBLE";
        case LONG:
          return "BIGINT";
        case DATE:
          return "DATE";
        case TIMESTAMP:
          return "TIMESTAMP(3)";
        case BOOLEAN:
          return "BOOLEAN";
        case BYTES:
          return "ANY(2147483647, 0)";
        case UUID:
          return "ANY(2147483647, 0)";
        case STRING:
          return "VARCHAR(5000)";
      }
    }
    return null;
  }

}
