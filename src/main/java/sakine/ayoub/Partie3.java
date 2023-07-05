package sakine.ayoub;

public class Partie3 {

    public static void main(String[] args) {

        SparkSession cp = SparkSession.builder.appName("app-vols").master("local[*]").getOrCreate();

        Dataset<Row> df = sparkSession.readStream().option("header","true").option("inferSchema","true").csv("hdfs://localhost:9000/rep1");

        df.createOrReplaceTempView("incidents");

        String query = "SELECT avion_id,COUNT(*) AS total_incidents" +
                "FROM incidents" +
                "GROUP BY avion_id" +
                "ORDER BY total_incidents DESC" +
                "LIMIT 1";

       Dataset<Row> result = cp.sql(query);

       StreamingQuery st = result.writeStream().outputMode("complete").format("console").trigger(Trigger.ProcessingTime(8000)).start();
    }
}
