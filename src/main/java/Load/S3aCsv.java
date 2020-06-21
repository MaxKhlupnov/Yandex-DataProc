package Load;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

public class S3aCsv {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTest");
        SparkContext sc = new SparkContext(conf);
        sc.setLogLevel("ERROR");

           sc.hadoopConfiguration().set("fs.s3a.endpoint", "storage.yandexcloud.net");
            sc.hadoopConfiguration().set("fs.s3a.access.key", SecretsConst.YOS_ACCESS_KEY);
            sc.hadoopConfiguration().set("fs.s3a.secret.key",SecretsConst.YOS_SECRET_KEY);


        SQLContext  sqlCs = new SQLContext(sc);

       Dataset<Row> csvFilesList = sqlCs.read()
                .option("header", "true")
                .option("partitionColumn","PartitionKey")
                .option("fetchsize","10000")
                .option("delimiter", ",")
                .schema(CsvSchemaType.CsvSchema)
                .csv(SecretsConst.YOS_BUCKET_PATH);
        System.out.println(csvFilesList.count());
        System.out.println(csvFilesList.schema().toString());

        csvFilesList.show();
        System.out.println("Hello, world!");
        if (csvFilesList.count() > 0) {

            csvFilesList.createOrReplaceTempView("tempCsvTelemetry");

            //Saving data to a JDBC source
            Properties connectionProperties = new Properties();
            //connectionProperties.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connectionProperties.put("driver", "org.postgresql.Driver");
            connectionProperties.put("user", SecretsConst.POSTGRE_USER);
            connectionProperties.put("password", SecretsConst.POSTGRE_PWD);


            csvFilesList.sqlContext()
                    .sql("SELECT cast(PartitionKey as varchar(50)) as device_id, cast(RowKey as int) as seconds_counter, Timestamp," +
                            " accel_pedal_position, alarm_event, altitude, ambient_air_temperature, battery_soc, battery_voltage   FROM tempCsvTelemetry")
                    .write() // Specifying create table column data types on write
                    .option("createTableColumnTypes","device_id  VARCHAR (50), seconds_counter INT, Timestamp TIMESTAMP," +
                            "    accel_pedal_position  INT, altitude  FLOAT, battery_voltage  FLOAT")
                    .option("batchsize","10000")
                    .mode("append")
                    .jdbc(SecretsConst.JDBC_PROVIDER_URL, "public.CsvImportAuto", connectionProperties);
        }
    }

    private static List<String> GetCsvFileList(SparkContext sc, String bucklePath) {
        JavaSparkContext context = new JavaSparkContext(sc);
        JavaPairRDD<String, String> wholeDirectoryRDD = context
                .wholeTextFiles(bucklePath,4);

        JavaRDD<String> lineCounts = wholeDirectoryRDD
                .filter(new Function<Tuple2<String, String>, Boolean>() { // Filter schema files
                    @Override
                    public Boolean call(Tuple2<String, String> fileNameContent) throws Exception {
                        return (!fileNameContent._1().endsWith(".schema.csv"));
                    }
                })
                .map(new Function<Tuple2<String, String>, String>(){
                    @Override
                    public String call(Tuple2<String, String> fileNameContent) throws Exception {
                        return fileNameContent._1;
                    }

                });

                return lineCounts.collect();
    }
}
