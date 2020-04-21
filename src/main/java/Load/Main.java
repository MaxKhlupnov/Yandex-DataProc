package Load;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");
        SparkContext sc = new SparkContext(conf);
            sc.hadoopConfiguration().set("fs.s3a.endpoint", "storage.yandexcloud.net");
            sc.hadoopConfiguration().set("fs.s3a.access.key", SecretsConst.YOS_ACCESS_KEY);
            sc.hadoopConfiguration().set("fs.s3a.secret.key",SecretsConst.YOS_SECRET_KEY);

        SQLContext  sqlCs = new SQLContext(sc);
        Dataset<Row> df = sqlCs
                    .read()
                    .option("header", "true")
                    .option("mode", "DROPMALFORMED")
                    .schema(CsvSchemaType.CsvSchema)
                    .csv(SecretsConst.YOS_BUCKET_PATH)
                .toDF();
        System.out.println(df.count());

        df.createOrReplaceTempView("csvTelemetry");
        df.sqlContext().sql("SELECT PartitionKey, count(RowKey) as RowsCount FROM csvTelemetry WHERE GROUP BY PartitionKey").show();

    }
}
