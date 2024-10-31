import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

import java.util.Properties

object ingest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Ingest Netflix")
      .master("local")
      .getOrCreate()


    val txt = new loadfile(spark)
    val dir = "/Users/eduardoalberto/LoadFile/netflix_titles.csv"
    val df = txt.arq(dir)
    val dfs = df.na.fill("unknown",Seq("country"))
                .na.fill("unknowm",Seq("cast"))
                .na.fill("unknowm",Seq("director"))

    val dfTrimmed = dfs.withColumn("date_string_trimmed", trim(col("date_added")))
    val dateRegex = "([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})"
    val dfWithZero = dfTrimmed.withColumn("date_string_padded", regexp_replace(col("date_string_trimmed"), "(\\b\\d{1}\\b),", "0$1,"))
    val dfFiltered = dfWithZero.withColumn("date", regexp_extract(col("date_string_padded"), dateRegex, 0))

    val dfFinal = dfFiltered.withColumn("date_formatted", date_format(to_date(col("date"), "MMMM dd, yyyy"), "yyyy-MM-dd"))
                            .select(col("show_id"),
                                    col("type"),
                                    col("title"),
                                    col("director"),
                                    col("cast"),
                                    col("country"),
                                    col("date_added"),
                                    col("release_year"),
                                    col("rating"),
                                    col("duration"),
                                    col("listed_in"),
                                    col("description"),
                                    col("date_formatted")
                            )

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "mysql")
    val url = "jdbc:mysql://localhost:3306/myDbUser"

    dfFinal.write.mode("overwrite").jdbc(url, "tb_netflix",properties)
    dfFinal.show()

    dfFinal.select("date_added").groupBy("date_added").count().distinct().show(1000)


    spark.sparkContext.setLogLevel("ERROR")
    spark.stop()
  }

}