package com.taosdata.example.jdbcTaosdemo.domain;

public final class JdbcTaosdemoConfig {

    //The host to connect to TDengine. Must insert one
    private String host;
    //The TCP/IP port number to use for the connection. Default is 6030.
    private int port = 6030;
    //The TDengine user name to use when connecting to the server. Default is 'root'
    private String user = "root";
    //The password to use when connecting to the server. Default is 'taosdata'
    private String password = "taosdata";

    //Destination database. Default is 'test'
    private String dbName = "taosdemo_test";
    //keep
    private int keep = 3650;
    //days
    private int days = 10;
    //Super table Name. Default is 'meters'
    private String stbName = "meters";
    //Table name prefix. Default is 'd'
    private String tbPrefix = "d";
    //The number of tables. Default is 10.
    private int numberOfTable = 10;
    //The number of records per table. Default is 2
    private int numberOfRecordsPerTable = 2;
    //The number of records per request. Default is 100
    private int numberOfValuesPerInsert = 100;
    //
    private int sleep = 1000;

    //The number of threads. Default is 1.
    private int numberOfThreads = 1;
    //Delete data. Default is false
    private boolean deleteTable = false;

    public static void printHelp() {
        System.out.println("Usage: java -jar JdbcTaosDemo.jar [OPTION...]");
        System.out.println("-host                    host                       The host to connect to TDengine. you must input one");
        System.out.println("-port                    port                       The TCP/IP port number to use for the connection. Default is 6030");
        System.out.println("-user                    user                       The TDengine user name to use when connecting to the server. Default is 'root'");
        System.out.println("-password                password                   The password to use when connecting to the server.Default is 'taosdata'");
        System.out.println("===========================================================================================");
        System.out.println("-database                database                   Destination database. Default is 'test'");
        System.out.println("-keep                    keep                       Destination database. Default is 'test'");
        System.out.println("-days                    days                       Destination database. Default is 'test'");
        System.out.println("===========================================================================================");
        System.out.println("-tablePrefix             tablePrefix                Table prefix name. Default is 'd'");
        System.out.println("-numOfTables             num_of_tables              The number of tables. Default is 10");
        System.out.println("-numOfRecordsPerTable    num_of_records_per_table   The number of records per table. Default is 2");
        System.out.println("-numOfValuesPerInsert    num_of_records_per_req     The number of records per request. Default is 100");
        System.out.println("-numOfThreads            num_of_threads             The number of threads. Default is 1");
        System.out.println("-deleteTableAfterTest    delete table               Delete data methods. Default is false");
        System.out.println("--help                   print this help list");
//        System.out.println("--infinite                       infinite insert mode");
    }

    /**
     * parse args from command line
     *
     * @param args command line args
     * @return JdbcTaosdemoConfig
     */
    public JdbcTaosdemoConfig(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if ("-host".equals(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-port".equals(args[i]) && i < args.length - 1) {
                port = Integer.parseInt(args[++i]);
            }
            if ("-user".equals(args[i]) && i < args.length - 1) {
                user = args[++i];
            }
            if ("-password".equals(args[i]) && i < args.length - 1) {
                password = args[++i];
            }
            if ("-database".equals(args[i]) && i < args.length - 1) {
                dbName = args[++i];
            }
            if ("-keep".equals(args[i]) && i < args.length - 1) {
                keep = Integer.parseInt(args[++i]);
            }
            if ("-days".equals(args[i]) && i < args.length - 1) {
                days = Integer.parseInt(args[++i]);
            }
            if ("-tablePrefix".equals(args[i]) && i < args.length - 1) {
                tbPrefix = args[++i];
            }
            if ("-numOfTables".equals(args[i]) && i < args.length - 1) {
                numberOfTable = Integer.parseInt(args[++i]);
            }
            if ("-numOfRecordsPerTable".equals(args[i]) && i < args.length - 1) {
                numberOfRecordsPerTable = Integer.parseInt(args[++i]);
            }
            if ("-numOfValuesPerInsert".equals(args[i]) && i < args.length - 1) {
                numberOfValuesPerInsert = Integer.parseInt(args[++i]);
            }
            if ("-numOfThreads".equals(args[i]) && i < args.length - 1) {
                numberOfThreads = Integer.parseInt(args[++i]);
            }
            if ("-D".equals(args[i]) && i < args.length - 1) {
                deleteTable = Boolean.parseBoolean(args[++i]);
            }
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDbName() {
        return dbName;
    }

    public int getKeep() {
        return keep;
    }

    public int getDays() {
        return days;
    }

    public String getStbName() {
        return stbName;
    }

    public String getTbPrefix() {
        return tbPrefix;
    }

    public int getNumberOfTable() {
        return numberOfTable;
    }

    public int getNumberOfRecordsPerTable() {
        return numberOfRecordsPerTable;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public boolean isDeleteTable() {
        return deleteTable;
    }

    public int getNumberOfValuesPerInsert() {
        return numberOfValuesPerInsert;
    }
}
