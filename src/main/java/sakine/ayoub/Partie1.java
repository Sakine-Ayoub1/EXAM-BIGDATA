package sakine.ayoub;

import java.util.Properties;

public class Partie1 {

    public static void main(String[] args) {
        SparkSession cp = SparkSession.builder.appName("app-vols").master("local[*]").getOrCreate();

        var connProp = new Properties();
        connProp.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        connProp.put("user", "root");
        connProp.put("password", "");

        //String jdbc_url = "jdbc:mysql://localhost:3306/DB_VOLS?user=root&password=&driver=com.mysql.jdbc.Driver";

        Dataset<Row> df_vols = cp.read.jdbc("jdbc:mysql://localhost:3306/DB_VOLS", "vols", connProp);
        Dataset<Row> df_passagers = cp.read.jdbc("jdbc:mysql://localhost:3306/DB_VOLS", "passagers", connProp);
        Dataset<Row> df_reservations = cp.read.jdbc("jdbc:mysql://localhost:3306/DB_VOLS", "reservations", connProp);

        //Dataset<Row> df_vols = cp.read.format("jdbc").option("url", jdbc_url).option("dbtable", "vols").load();
        //Dataset<Row> df_passagers = cp.read.format("jdbc").option("url", jdbc_url).option("dbtable", "passagers").load();
        //Dataset<Row> df_reservations = cp.read.format("jdbc").option("url", jdbc_url).option("dbtable", "reservation").load();

        df_vols.createOrReplaceTempView("vols");
        df_passagers.createOrReplaceTempView("passagers");
        df_reservations.createOrReplaceTempView("reservations");

        String query = "SELECT vols.ID_VOL, vols.DATE_DEPART, COUNT(reservations.ID_PASSAGER) AS NOMBRE " +
                "FROM vols JOIN reservations ON vols.ID_VOL = reservations.ID_VOL JOIN passagers ON passagers.ID_PASSAGER = reservations.ID_PASSAGER " +
                "GROUP BY vols.ID_VOL, vols.DATE_DEPART ORDER BY vols.ID_VOL";

        cp.sql(query).show();

        String query2 = " SELECT vols.ID_VOL, vols.DATE_DEPART, vols.DATE_ARRIVE" +
                "    FROM vols" +
                "    WHERE vols.DATE_DEPART <= CURRENT_DATE() AND vols.DATE_ARRIVE >= CURRENT_DATE()";

        cp.sql(query2).show();
    }
}
