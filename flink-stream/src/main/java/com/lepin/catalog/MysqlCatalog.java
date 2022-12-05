package com.lepin.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Catalog for MySql.
 */
@Internal
public class MysqlCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlCatalog.class);

    public MysqlCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    // ------ Mysql default objects that shouldn't be exposed to users ------

    private static final Set<String> builtinDatabases = new HashSet<String>() {{
        add("information_schema");
        add("performance_schema");
        add("mysql");
        add("innodb");
        add("sys");
    }};


    // ------ databases ------
    @Override
    public List<String> listDatabases() throws CatalogException {

        List<String> mysqlDatabases = new ArrayList<>();
        try{
            Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
            try{
                PreparedStatement ps = conn.prepareStatement("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA;");
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String dbName = rs.getString(1);
                    if (!builtinDatabases.contains(dbName)) {
                        mysqlDatabases.add(rs.getString(1));
                    }
                }
                return mysqlDatabases;
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed listing database in catalog %s", getName()), e);
            }finally {
                if (conn != null) {
                    conn.close();
                }
            }
        }catch (Exception exception){
            throw new CatalogException(String.format("Failed listing database in catalog %s", this.getName()), exception);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (this.listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), (String)null);
        } else {
            throw new DatabaseNotExistException(this.getName(), databaseName);
        }
    }

    // ------ tables ------
    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!this.databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.getName(), databaseName);
        }else{
            try {
                Connection conn = DriverManager.getConnection(this.baseUrl + databaseName, this.username, this.pwd);
                try{
                    PreparedStatement ps = conn.prepareStatement("select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA = ?");
                    ps.setString(1,databaseName);
                    ResultSet rs = ps.executeQuery();
                    List<String> tables = new ArrayList<>();
                    while(rs.next()) {
                        String table = rs.getString(1);
                        tables.add(String.format("%s.%s",databaseName,table));
                    }
                    return tables;
                }catch (Throwable throwables){
                    throw throwables;
                }
            } catch (SQLException throwables) {
                throw new CatalogException(String.format("Failed listing database in catalog %s", this.getName()), throwables);
            }

        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!this.tableExists(tablePath)) {
            throw new TableNotExistException(this.getName(), tablePath);
        }else {
            MysqlTablePath mysqlPath = MysqlTablePath.fromFlinkTableName(tablePath.getObjectName());
            String dbUrl = this.baseUrl + tablePath.getDatabaseName();

            try {
                Connection conn = DriverManager.getConnection(dbUrl, this.username, this.pwd);
                Throwable var5 = null;

                CatalogTableImpl var15;
                try {
                    DatabaseMetaData metaData = conn.getMetaData();
                    Optional<UniqueConstraint> primaryKey = this.getPrimaryKey(metaData, mysqlPath.getMysqlDataBaseName(), mysqlPath.getMysqlTableName());
                    PreparedStatement ps = conn.prepareStatement(String.format("SELECT * FROM %s;", mysqlPath.getFullPath()));
                    ResultSetMetaData rsmd = ps.getMetaData();
                    String[] names = new String[rsmd.getColumnCount()];
                    DataType[] types = new DataType[rsmd.getColumnCount()];

                    for(int i = 1; i <= rsmd.getColumnCount(); ++i) {
                        names[i - 1] = rsmd.getColumnName(i);
                        types[i - 1] = this.convertJDBCType(rsmd, i);
                        if (rsmd.isNullable(i) == 0) {
                            types[i - 1] = (DataType)types[i - 1].notNull();
                        }
                    }

                    TableSchema.Builder tableBuilder = (new TableSchema.Builder()).fields(names, types);
                    primaryKey.ifPresent((pk) -> {
                        tableBuilder.primaryKey(pk.getName(), (String[])pk.getColumns().toArray(new String[0]));
                    });
                    TableSchema tableSchema = tableBuilder.build();
                    Map<String, String> props = new HashMap();
                    props.put(FactoryUtil.CONNECTOR.key(), "jdbc");
                    props.put("url", dbUrl);
                    props.put("table-name", mysqlPath.getFullPath());
                    props.put("username", this.username);
                    props.put("password", this.pwd);
                    var15 = new CatalogTableImpl(tableSchema, props, "");
                } catch (Throwable var25) {
                    var5 = var25;
                    throw var25;
                } finally {
                    if (conn != null) {
                        if (var5 != null) {
                            try {
                                conn.close();
                            } catch (Throwable var24) {
                                var5.addSuppressed(var24);
                            }
                        } else {
                            conn.close();
                        }
                    }

                }

                return var15;
            } catch (Exception var27) {
                LOG.error(String.format("Failed getting table %s", tablePath.getFullName()));
                throw new CatalogException(String.format("Failed getting table %s", tablePath.getFullName()), var27);
            }
        }
    }

    public DataType convertJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex);
        DataType columnType = DataTypes.STRING();
        if (Objects.isNull(mysqlType)) {
            return columnType;
        }
        if (mysqlType.contains("CHAR") || mysqlType.contains("VARCHAR") || mysqlType.contains("TEXT") ||
                mysqlType.contains("NCHAR") || mysqlType.contains("NVARCHAR") || mysqlType.contains("NTEXT")
                || mysqlType.contains("UNIQUEIDENTIFIER") || mysqlType.contains("SQL_VARIANT")) {
            columnType = DataTypes.STRING();
        } else if (mysqlType.contains("BIGINT")) {
            columnType = DataTypes.BIGINT();
        } else if (mysqlType.contains("INT") || mysqlType.contains("TINYINT") || mysqlType.contains("SMALLINT") || mysqlType.contains("INTEGER")) {
            columnType = DataTypes.INT();
        } else if (mysqlType.contains("FLOAT")) {
            columnType = DataTypes.FLOAT();
        } else if (mysqlType.contains("DECIMAL") || mysqlType.contains("MONEY") || mysqlType.contains("SMALLMONEY")
                || mysqlType.contains("NUMERIC")) {
            columnType = DataTypes.DECIMAL(38,20);
        } else if (mysqlType.contains("DOUBLE")) {
            columnType = DataTypes.DOUBLE();
        } else if (mysqlType.contains("BOOLEAN")) {
            columnType = DataTypes.BOOLEAN();
        } else if (mysqlType.contains("SMALLDATETIME") || mysqlType.contains("DATETIME")) {
            columnType = DataTypes.TIMESTAMP();
        } else if (mysqlType.contains("TIMESTAMP") || mysqlType.contains("BINARY") || mysqlType.contains("VARBINARY") || mysqlType.contains("IMAGE")) {
            columnType = DataTypes.BYTES();
        } else if (mysqlType.contains("TIME")) {
            columnType = DataTypes.TIME();
        } else if (mysqlType.contains("DATE")) {
            columnType = DataTypes.DATE();
        }
        return columnType;
    }


    private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String mysqlType = metadata.getColumnTypeName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        byte var7 = -1;
//        LOG.error("hash code : {} ,mysql type:{}", mysqlType.hashCode(), mysqlType);
        switch(mysqlType.hashCode()) {
            case -594415409:
                if (mysqlType.equals("TINYINT")) {
                    var7 = 0;
                }
                break;
            case 176095624:
                if (mysqlType.equals("SMALLINT")) {
                    var7 = 1;
                }
                break;
            case 72655:
                if (mysqlType.equals("INT")) {
                    var7 = 2;
                }
                break;
            case 1959128815:
                if (mysqlType.equals("BIGINT")) {
                    var7 = 3;
                }
                break;
            case 66988604:
                if (mysqlType.equals("FLOAT")) {
                    var7 = 4;
                }
                break;
            case 2022338513:
                if (mysqlType.equals("DOUBLE")) {
                    var7 = 5;
                }
                break;
            case -2034720975:
                if (mysqlType.equals("DECIMAL")) {
                    var7 = 6;
                }
                break;
            case 2090926:
                if (mysqlType.equals("DATE")) {
                    var7 = 7;
                }
                break;
            case 2575053:
                if (mysqlType.equals("TIME")) {
                    var7 = 8;
                }
                break;
            case -1718637701:
                if (mysqlType.equals("DATETIME")) {
                    var7 = 9;
                }
                break;
            case -1453246218:
                if (mysqlType.equals("TIMESTAMP")) {
                    var7 = 10;
                }
                break;
            case 2067286:
                if (mysqlType.equals("CHAR")) {
                    var7 = 11;
                }
                break;
            case 954596061:
                if (mysqlType.equals("VARCHAR")) {
                    var7 = 12;
                }
                break;
            case 651290682:
                if (mysqlType.equals("MEDIUMINT")) {
                    var7 = 13;
                }
                break;
            case 2719805:
                if (mysqlType.equals("YEAR")) {
                    var7 = 14;
                }
                break;
            case -1247219043:
                if (mysqlType.equals("TINYBLOB")) {
                    var7 = 15;
                }
                break;
            case 2041757:
                if (mysqlType.equals("BLOB")) {
                    var7 = 16;
                }
                break;
            case -1285035886:
                if (mysqlType.equals("MEDIUMBLO")) {
                    var7 = 17;
                }
                break;
            case -1291368423:
                if (mysqlType.equals("LONGBLOB")) {
                    var7 = 18;
                }
                break;
            case 65773:
                if (mysqlType.equals("BIT")) {
                    var7 = 19;
                }
                break;
            case 651601158:
                if (mysqlType.equals("BIGINT UNSIGNED")) {
                    var7 = 20;
                }
                break;
            case 1840247846:
                if (mysqlType.equals("INT UNSIGNED")) {
                    var7 = 21;
                }
                break;
            case -834748634:
                if (mysqlType.equals("TINYINT UNSIGNED")) {
                    var7 = 22;
                }
                break;
            case -1646715132:
                if (mysqlType.equals("DECIMAL UNSIGNED")) {
                    var7 = 23;
                }
                break;
            case 454454925:
                if (mysqlType.equals("SMALLINT UNSIGNED")) {
                    var7 = 24;
                }
                break;
            case 2571565:
                if (mysqlType.equals("TEXT")) {
                    var7 = 25;
                }
                break;
            case -1290838615:
                if (mysqlType.equals("LONGTEXT")) {
                    var7 = 26;
                }
        }

        switch(var7) {
            case 0:
                return DataTypes.TINYINT();
            case 1:
                return DataTypes.SMALLINT();
            case 2:
            case 13:
                return DataTypes.INT();
            case 3:
                return DataTypes.BIGINT();
            case 4:
                return DataTypes.FLOAT();
            case 5:
                return DataTypes.DOUBLE();
            case 6:
                return DataTypes.DECIMAL(38,18);
            case 7:
                return DataTypes.DATE();
            case 8:
            case 9:
            case 10:
                return DataTypes.TIMESTAMP(scale);
            case 11:
                return DataTypes.CHAR(precision);
            case 12:
                return DataTypes.VARCHAR(precision);
            case 19:
                return DataTypes.TINYINT();
            case 20:
                return DataTypes.BIGINT();
            case 21:
                return DataTypes.INT();
            case 22:
                return DataTypes.TINYINT();
            case 23:
                return DataTypes.DECIMAL(38,18);
            case 24:
                return DataTypes.INT();
            case 25:
                return DataTypes.STRING();
            case 26:
                return DataTypes.STRING();
            default:
                throw new UnsupportedOperationException(String.format("Doesn't support MySql type '%s' yet", mysqlType));
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List tables = null;

        try {
            tables = this.listTables(tablePath.getDatabaseName());

        } catch (DatabaseNotExistException var4) {
            return false;
        }
        return tables.contains(MysqlTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath());
    }
}
