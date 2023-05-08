package com.migber
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import java.io.File
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.DataFrame

object GoogleSparkProcessing extends App {

  // WINDOWS CONFIGURATION
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  // START SPARK CONTEXT
  val sc = SparkSession
    .builder()
    //.master("local[1]")
    .appName("GoogleSparkProcessing")
    .config("spark.master", "local")
    .config("spark.sql.parquet.compression.codec", "gzip")
    .enableHiveSupport()
    .getOrCreate()

  import sc.implicits._

  println("APP Name :" + sc.sparkContext.appName)
  //println("Deploy Mode :" + sc.sparkContext.deployMode)
  //println("Master :" + sc.sparkContext.master)

  // SUPPORT FUNCTIONS
  def csvReader(path: String)(spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(path)
  }
  def parser(name: String)(df: DataFrame): DataFrame = {
    df.withColumn(name, regexp_replace(col(name), "\\++", "PlusPlus"))
      .withColumn(name, trim(regexp_replace(col(name), "\\W", "")))
  }

  def csvWriter(name: String)(folder: String)(delimiter: String)(df: DataFrame) = {

    val src = folder + "tmp"
    val dst = folder + name

    // GENERATE ONLY ONE .CSV PART FILE
    df.coalesce(1)
      .write
      .options(Map("header" -> "true", "delimiter" -> delimiter))
      .csv(src)

    // GET .CSV PART FILE
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val srcPath = new Path(src)
    val dstPath = new Path(dst)
    val srcFile = FileUtil.listFiles(new File(src))
      .filter(f => f.getPath.endsWith(".csv"))(0)

    // RENAME AND COPY .CSV PART FILE TO DIR + DELETE TEMP FILES
    FileUtil.copy(srcFile, hdfs, dstPath, false, hadoopConfig)
    val delFile = folder + "." + name + ".crc"
    hdfs.delete(new Path(delFile), true)
    hdfs.delete(srcPath, true)

  }

  def parquetWriter(name: String)(folder: String)(df: DataFrame) = {

    val src = folder + "tmp"
    val dst = folder + name

    // GENERATE ONLY ONE .PARQUET PART FILE
    df.coalesce(1)
      .write
      .parquet(src)

    // GET .PARQUET PART FILE
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val srcPath = new Path(src)
    val dstPath = new Path(dst)
    val srcFile = FileUtil.listFiles(new File(src))
      .filter(f => f.getPath.endsWith(".parquet"))(0)

    // RENAME AND COPY .PARQUET PART FILE TO DIR + DELETE TEMP FILES
    FileUtil.copy(srcFile, hdfs, dstPath, false, hadoopConfig)
    val delFile = folder + "." + name + ".crc"
    hdfs.delete(new Path(delFile), true)
    hdfs.delete(srcPath, true)

  }

  // TASK 1
  val googleurPath = "src/data/googleplaystore_user_reviews.csv"

  var df_1 = csvReader(googleurPath)(sc)
    .select(col("App").cast("string"), col("Sentiment_Polarity").cast("double"))
    .na.fill(0, Array("Sentiment_Polarity"))
    .transform(parser("App"))
    .groupBy("App")
    .agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))
    .sort(col("App"))

  // CHECKS
  df_1.printSchema()
  df_1.show(5)
  println(df_1.count())

  // TASK 2
  val googlepsPath = "src/data/googleplaystore.csv"

  var df_2 = csvReader(googlepsPath)(sc)
    .select(col("App").cast("string"), col("Rating").cast("double"))
    .filter(!$"Rating".isNaN && $"Rating" >= 4.0)
    .transform(parser("App"))
    .dropDuplicates("App")
    .filter(!$"Genres".contains("February"))
    .sort(col("Rating").desc)


  // CHECKS
  df_2.printSchema()
  df_2.show(5)
  println(df_2.count())

  // WRITE OUTPUT
  csvWriter("best_apps.csv")("src/data/out/")("§")(df_2)

  // TASK 3
  var df_3_base = csvReader(googlepsPath)(sc)
    .transform(parser("App"))
    .filter(!$"Genres".contains("February"))


  var df_3_cats = df_3_base.groupBy(col("App"))
    .agg(collect_set($"Category").as("Categories"))


  val reviewsPartition = Window.partitionBy("App").orderBy(col("Reviews").cast("long").desc)

  var df_3 = df_3_base.join(df_3_cats, Seq("App"), "left")
    .withColumn("row", row_number.over(reviewsPartition))
    .where($"row" === 1)
    .drop(col("row"))
    .withColumn("Genres", split(col("Genres"), ";"))
    .withColumn("Size", regexp_replace(col("Size"), "M", ""))
    .withColumn("Last Updated", to_timestamp(col("Last Updated"), "MMMM d, yyyy"))
    .withColumn("Price", trim(regexp_replace(col("Price").cast("string"), "\\$", "")))
    .withColumn("Price", col("Price").cast("double") * lit(0.9))
    .withColumn("Reviews", col("Reviews").cast("long"))
    .withColumn("Current Ver", regexp_replace(col("Current Ver"), "DPSTATUS", ""))
    .na.fill(0, Array("Reviews"))
    .filter(!($"App"===""))
    .filter(!$"Rating".isNaN)
    .dropDuplicates("App")
    .select(
      col("App").cast("string"),
      col("Categories"),
      col("Rating").cast("double"),
      col("Reviews"),
      col("Size").cast("double"),
      col("Installs").cast("string"),
      col("Type").cast("string"),
      col("Price"),
      col("Content Rating").cast("string").as("Content_Rating"),
      col("Genres"),
      col("Last Updated").cast("date").as("Last_Updated"),
      col("Current Ver").cast("string").as("Current_Version"),
      col("Android Ver").cast("string").as("Minimum_Android_Version"),
    )

  // CHECKS
  df_3.printSchema()
  df_3.show(5)
  println(df_3.count())

  // TASK 4
  df_3 = df_3.join(df_1, Seq("App"), "left")

  // CHECKS
  df_3.printSchema()
  df_3.filter(col("Average_Sentiment_Polarity").isNotNull).show(5)
  println(df_3.count())
  println(df_3.filter(col("Average_Sentiment_Polarity").isNull || col("Average_Sentiment_Polarity") === "").count())

  // WRITE OUTPUT
  parquetWriter("googleplaystore_cleaned.parquet")("src/data/out/")(df_3)

  // TASK 5
  var df_4 = df_3.select(
    explode($"Genres").as("Genres"),
    col("App"),
    col("Rating"),
    col("Average_Sentiment_Polarity"),
  )
    .groupBy("Genres")
    .agg(count("App").as("Count"), avg("Rating").as("Average_Rating"), sum("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity"))

  // CHECKS
  df_4.printSchema()
  df_4.show(5)
  println(df_4.count())

  // WRITE OUTPUT
  parquetWriter("googleplaystore_metrics.parquet")("src/data/out/")(df_4)

  // STOP SPARK CONTEXT
  sc.sparkContext.stop()
}
