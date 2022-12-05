package com.lepin.options;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DbSourceOptions {

    public static final ConfigOption<String> MYSQL_CATALOG =
            ConfigOptions.key("mysql-catalog")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("mysql catalog of the database server.");

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the database to use when connecting to the database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the database server.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the database to monitor.");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Schema name of the database to monitor.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the database to monitor.");

    public static final ConfigOption<String> SERVER_ID =
            ConfigOptions.key("server-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A numeric ID or a numeric ID range of this database client, "
                                    + "The numeric ID syntax is like '5400', the numeric ID range syntax "
                                    + "is like '5400-5408', The numeric ID range syntax is recommended when "
                                    + "'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all "
                                    + "currently-running database processes in the MySQL cluster. This connector"
                                    + " joins the MySQL  cluster as another server (with this unique ID) "
                                    + "so it can read the binlog. By default, a random number is generated between"
                                    + " 5400 and 6400, though we recommend setting an explicit value.");


    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("Schema name of the database to monitor.");

}
