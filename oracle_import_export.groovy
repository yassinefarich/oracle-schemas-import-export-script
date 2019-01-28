import groovy.sql.Sql

/**
 * <p>Oracle Schemas Import/Export Script
 * Based on https://rolfje.wordpress.com/2015/01/02/importexport-an-oracle-schema-using-jdbc/ </p>
 * 
 * @author Yassine FARICH <yassinefarich@gmail.com>
 */
class Main {

    // GLOBAL PARAMETERS
    static final ORACLE_HOST = '192.168.50.4'
    static final ORACLE_PORT = '1521'
    static final ORACLE_SID = 'XE'
    static final ORACLE_USER = 'system'
    static final ORACLE_PASSWORD = 'manager'
    static final JDBC_DRIVER_JAR = 'ojdbc6.jar'

    static final SCHEMAS_TO_EXPORT = ['HR']
    static final EXPORT_DIR = "/vagrant"


    static void main(String[] args) {
        new Main().start();
        //new Main().start(args.length > 0 && "IMPORT".equals(args[0]) ? "IMPORT" : "EXPORT");
    }

    void start(def scriptMode = "EXPORT") {

        initJDBCDataSource()

        if ("EXPORT".equals(scriptMode))
            SCHEMAS_TO_EXPORT.forEach {
                schemaToExport ->
                    if (schemas.contains(schemaToExport)) {
                        exportSchema("${schemaToExport}", "${schemaToExport}.dump")
                    } else println "Cannot find schema ${schemaToExport} on this database"
            }

        if ("IMPORT".equals(scriptMode))
            SCHEMAS_TO_EXPORT.forEach {
                schemaToImport ->
                    if (!schemas.contains(schemaToImport)) {
                        importSchema("${schemaToImport}", "${schemaToImport}.dump")
                    } else println "Schema ${schemaToImport} is already existant !!!"
            }

    }

    def initJDBCDataSource() {

        // LoadJDBC Driver
        this.getClass().classLoader.rootLoader.addURL(new File(JDBC_DRIVER_JAR).toURL())

        def connexionString = "jdbc:oracle:thin:@${ORACLE_HOST}:${ORACLE_PORT}:${ORACLE_SID}"
        def dataSourceClass = "oracle.jdbc.pool.OracleDataSource"

        println "Connecting to Oracle database ..."
        dataSource = Sql.newInstance(connexionString, ORACLE_USER, ORACLE_PASSWORD, dataSourceClass)
        println "Connexion OK \n"
        println "Listing schemas : "

        schemas = [];
        dataSource.eachRow("select USERNAME from SYS.ALL_USERS")
                {
                    name ->
                        println " - ${name.userName}"
                        schemas.add(name.userName);
                }
    }

    void exportSchema(String schema, String fileName) {
        println "Start exporting schema ${schema} : file ${fileName} this will take some time ..."
        def pStmt = dataSource
                .getConnection()
                .prepareStatement(EXPORT_SQL_SCRIPT);

        pStmt.setString(1, schema.toUpperCase());
        pStmt.setString(2, EXPORT_DIR);
        pStmt.setString(3, fileName);
        pStmt.execute();
        println "Exporting schema ${schema} has been done :)"
    }

    void importSchema(String schemaName, String fileName) {
        println "Start importing schema ${schemaName} : file ${fileName} this will take some time ..."
        def pStmt = dataSource
                .getConnection()
                .prepareStatement(IMPORT_SQL_SCRIPT);
        pStmt.setString(1, schemaName.toUpperCase());
        pStmt.setString(2, EXPORT_DIR);
        pStmt.setString(3, fileName);
        pStmt.execute();
        println "Importing schema ${schemaName} has been done :)"
    }

    // ************** intern variables ********************
    private ArrayList<String> schemas;
    private Sql dataSource;

    final IMPORT_SQL_SCRIPT = """
         -- IMPORT SQL SCRIPT
         DECLARE
           schemaname        VARCHAR2(200) := ?;
           directoryname     VARCHAR2(200) := ?;
           dumpfilename      VARCHAR2(200) := ?;
          
           directoryvariable VARCHAR(100) := 'EXPORT_DIR_'
                             || schemaname;
                              
           handle            NUMBER;
           status            VARCHAR2(20);
         BEGIN
          
           EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY '
                             || directoryvariable || ' AS '''
                             || directoryname || '''';
          
           handle := DBMS_DATAPUMP.OPEN(
             operation => 'IMPORT',
             job_mode  => 'SCHEMA',
             job_name  => 'datapump import schema '
                          || schemaname);
          
           DBMS_DATAPUMP.ADD_FILE(
             handle    => handle,
             filename  => dumpfilename,
             directory => directoryvariable);
          
           DBMS_DATAPUMP.ADD_FILE(
             handle    => handle,
             filename  => dumpfilename || '.import.log',
             directory => directoryvariable,
             filetype  => DBMS_DATAPUMP.KU\$_FILE_TYPE_LOG_FILE,
             reusefile => 1);
          
           DBMS_DATAPUMP.SET_PARAMETER(
             handle => handle,
             name   => 'table_exists_action',
             value  => 'REPLACE');
          
           DBMS_DATAPUMP.METADATA_FILTER(
             handle => handle,
             name   => 'SCHEMA_EXPR',
             value  => 'IN (''' || schemaname || ''')');
          
           DBMS_DATAPUMP.START_JOB(handle);
           DBMS_DATAPUMP.WAIT_FOR_JOB(handle, status);
          
           EXECUTE IMMEDIATE 'DROP DIRECTORY '
                             || directoryvariable;
         END;
     """

    final EXPORT_SQL_SCRIPT = """
        -- EXPORT SQL SCRIPT
        DECLARE
          schemaname    VARCHAR2(200) := ?;
          directoryname VARCHAR2(200) := ?;
          dumpfilename  VARCHAR2(200) := ?;
         
          directoryvariable VARCHAR(100) 
                            := 'EXPORT_DIR_' || schemaname;
         
          handle            NUMBER;
          status            VARCHAR2(20);
        BEGIN
         
          EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY '
                            || directoryvariable || ' AS '''
                            || directoryname || '''';
         
          handle := DBMS_DATAPUMP.OPEN(
            operation => 'EXPORT',
            job_mode  => 'SCHEMA',
            job_name  => 'datapump export schema '
                         || schemaname);
         
          DBMS_DATAPUMP.ADD_FILE(
            handle    => handle,
            filename  => dumpfilename,
            directory => directoryvariable,
            reusefile => 1);
         
          DBMS_DATAPUMP.ADD_FILE(
            handle    => handle,
            filename  => dumpfilename || '.export.log',
            directory => directoryvariable,
            filetype  => DBMS_DATAPUMP.KU\$_FILE_TYPE_LOG_FILE,
            reusefile => 1);
         
          DBMS_DATAPUMP.METADATA_FILTER(
            handle => handle,
            name   => 'SCHEMA_EXPR',
            value  => 'IN (''' || schemaname || ''')');
         
          DBMS_DATAPUMP.START_JOB(handle);
          DBMS_DATAPUMP.WAIT_FOR_JOB(handle, status);
         
          EXECUTE IMMEDIATE 'DROP DIRECTORY '
                            || directoryvariable;
        END;
    """
}





