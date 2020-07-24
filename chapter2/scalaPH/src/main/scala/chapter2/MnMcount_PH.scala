package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCount {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("MnMCount_PH")
            .getOrCreate()

        if (args.length < 1) {
            print("Usage: MnMCount <mnm_file_dataset>")
            sys.exit(1)
        }

        // Get M&M data set filename
        val mnmFile = args(0)

        // Read into a Spark DF
        val mnmDF = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(mnmFile)

        // Aggregate
        val countMnMDF = mnmDF
            .select("State", "Color", "Count")
            .groupBy("State", "Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))

        countMnMDF.show(60)
        println(s"Total Rows = ${countMnMDF.count()}")
        println()

        val calCountMnMDF = mnmDF
            .select("State", "Color", "Count")
            .where(col("State") == "CA")
            .groupBy("State", "Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))

         calCountMnMDF.show(10)

         spark.stop()
    }
}