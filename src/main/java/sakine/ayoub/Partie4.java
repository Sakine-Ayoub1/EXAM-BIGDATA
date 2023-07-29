package sakine.ayoub;

import static org.apache.spark.sql.functions.*;

public class partie4 {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("Incident Streaming Analysis")
                .getOrCreate();

        // Définition du schéma des données dans les fichiers CSV
        String schemaString = "id description no_avion date";
        StructType schema = new StructType();
        for (String fieldName : schemaString.split(" ")) {
            schema = schema.add(fieldName, "string");
        }

        // Lecture des données en streaming à partir des fichiers CSV sur HDFS
        Dataset<Row> incidentsDF = spark.readStream()
                .option("header", "false")
                .schema(schema)
                .csv("/fichier_incidents_avion_csv");

        // Tâche 1 : Afficher d'une manière continue l'avion ayant plus d'incidents
        Dataset<Row> incidentsCountByAvion = incidentsDF.groupBy("no_avion").count();
        StreamingQuery query1 = incidentsCountByAvion.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Tâche 2 : Afficher d'une manière continue les deux mois de l'année en cours où il y avait moins d'incidents
        Dataset<Row> incidentsByMonth = incidentsDF.withColumn("month", month(to_date(col("date"), "yyyy-MM-dd")));
        Dataset<Row> incidentsCountByMonth = incidentsByMonth.groupBy("month").count();
        StreamingQuery query2 = incidentsCountByMonth.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Démarrer le traitement en streaming
        query1.awaitTermination();
        query2.awaitTermination();
    }
}
