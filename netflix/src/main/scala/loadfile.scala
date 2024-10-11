import org.apache.spark.sql._
import org.apache.spark.sql.types.{BooleanType, DateType, FloatType, IntegerType, StringType, StructType}

class loadfile(spark: SparkSession){
  def arq(arq:String): DataFrame ={

    val schema = new StructType()
      .add("show_id",StringType, nullable = true)
      .add("type",StringType, nullable = true)
      .add("title",StringType, nullable = true)
      .add("director",StringType, nullable = true)
      .add("cast",StringType, nullable = true)
      .add("country",StringType, nullable = true)
      .add("date_added",StringType, nullable = true)
      .add("release_year",DateType, nullable = true)
      .add("rating",StringType, nullable = true)
      .add("duration",StringType, nullable = true)
      .add("listed_in",StringType, nullable = true)
      .add("description",StringType, nullable = true)


    val df = spark.read
      .schema(schema)
      .option("header","true")
      .option("delimiter","\t")
      .option("sep",",")
      .csv(arq)
    df
  }
}