package main;

import org.apache.hive.jdbc.HiveDriver;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class ExportStats {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        String hiveUrl = "";
        String user = "";
        String pwd = "";
        String interval = "";
        String path = "";
        try {
            if(args.length != 5){
                throw new Exception("Incorrect number of arguments\nUsage: [...].jar " +
                        "<hive_url> <user> <password> <interval_sec> <exp_path>");
            }
            hiveUrl = args[0];
            user = args[1];
            pwd = args[2];
            interval = args[3];
            path = args[4];
        } catch (Exception e) {
            System.out.print(e);
	    System.exit(1);
        }

        Connection con = DriverManager.getConnection(hiveUrl, user, pwd);

        Statement stmt = con.createStatement();
        String tableName = "xg_hbase_logs";

        String query = String.format("WITH log_time AS (SELECT * FROM %s WHERE hbase_ts >= (unix_timestamp()-%s)),\n", tableName, interval) +
                "req1 AS (SELECT request, ip FROM log_time WHERE status >= 300 AND status < 400),\n" +
                "req2 AS (SELECT request, ip FROM log_time WHERE status >= 400 AND status < 500),\n" +
                "req3 AS (SELECT request, ip FROM log_time WHERE status >= 500),\n" +
                "req4 AS (SELECT request, ip, size FROM log_time)\n" +
                String.format("INSERT OVERWRITE DIRECTORY '%s'\n", path) +
                "ROW FORMAT DELIMITED\n" +
                "FIELDS TERMINATED BY ','\n" +
                "SELECT COALESCE(req1.ip, req2.ip, req3.ip, req4.ip), count(req4.request), avg(req4.size),\n" +
                "count(req1.request), count(req2.request), count(req3.request)\n" +
                "FROM req1\n" +
                "FULL JOIN req2 ON req1.ip = req2.ip\n" +
                "FULL JOIN req3 ON req3.ip = COALESCE(req1.ip, req2.ip)\n" +
                "FULL JOIN req4 ON req4.ip = COALESCE(req1.ip, req2.ip, req3.ip)\n" +
                "GROUP BY req1.ip, req2.ip, req3.ip, req4.ip";

        ResultSet res = stmt.executeQuery(query);

        if (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
