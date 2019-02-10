import groovy.sql.Sql

import java.util.function.Consumer

/**
 * @author Yassine FARICH <yassinefarich@gmail.com>
 */
class Main {

    // ORACLE CONNECTION PROPERTIES
    static final ORACLE_HOST = '192.168.50.4'
    static final ORACLE_PORT = '1521'
    static final ORACLE_SID = 'XE'
    static final ORACLE_USER = 'system'
    static final ORACLE_PASSWORD = 'manager'
    static final JDBC_DRIVER_JAR = 'ojdbc6.jar'

    // ------------------------------------------

    static void main(String[] args) {
        new Main().run('.')
    }

    private run(String directoryName = '.') {
        def currentDir = new File(directoryName)

        processWithDataSource {
            dataSource ->
                currentDir
                        .listFiles()
                        .findAll { it.isFile() && it.name.toLowerCase().contains('.sql') }
                        .forEach { fileName -> 
                            println("Excuting $fileName")    
                            dataSource.executeUpdate(fileName.text)}
        }
    }

    private processWithDataSource(Consumer<Sql> sqlBehavior,Boolean loadJDBCDriver = true) {

        if (loadJDBCDriver) {
            this.getClass().classLoader.rootLoader.addURL(new File(JDBC_DRIVER_JAR).toURL())
        }

        def connexionString = "jdbc:oracle:thin:@${ORACLE_HOST}:${ORACLE_PORT}:${ORACLE_SID}"
        def dataSourceClass = "oracle.jdbc.pool.OracleDataSource"

        println "Connecting to Oracle database ..."
        Sql dataSource = Sql.newInstance(connexionString, ORACLE_USER, ORACLE_PASSWORD, dataSourceClass)
        println "Connexion OK :)"
        sqlBehavior.accept(dataSource)
        dataSource.close()
    }

}




