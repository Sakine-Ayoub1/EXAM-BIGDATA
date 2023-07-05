package sakine.ayoub;

public class Partie2 {

    // 1. sqoop import --connect "jdbc:mysql://localhost:3306/DB_AEROPORT" --username "root" --password "" --table "vols" --target-dir /sqoop

    // 2. sqoop export --connect "jdbc:mysql://localhost:3306/DB_AEROPORT" --username "root" --password "" --table "vols" --export-dir /vols.txt
    //    --input-fields-terminated-by "," --input-lines-terminated-by "/n"
}
