import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PokemonAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Pokemon Type Analysis")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Define schema of the CSV file
    val schema = new StructType()
      .add("#", IntegerType, nullable = true)
      .add("Name", StringType, nullable = true)
      .add("Type1", StringType, nullable = true)
      .add("Type2", StringType, nullable = true)
      .add("Total", IntegerType, nullable = true)
      .add("HP", IntegerType, nullable = true)
      .add("Attack", IntegerType, nullable = true)
      .add("Defense", IntegerType, nullable = true)
      .add("SpAtk", IntegerType, nullable = true)
      .add("SpDef", IntegerType, nullable = true)
      .add("Speed", IntegerType, nullable = true)
      .add("Generation", IntegerType, nullable = true)
      .add("Legendary", BooleanType, nullable = true)

    // Load CSV data
    val pokemonDF = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("hdfs:///data/pokemon.csv")

    // Calculate mean of HP, Attack, Defense, and Speed for each Type1
    val analysisDF = pokemonDF.groupBy("Type1")
      .agg(
        mean("HP").as("Mean_HP"),
        mean("Attack").as("Mean_Attack"),
        mean("Defense").as("Mean_Defense"),
        mean("Speed").as("Mean_Speed")
      )

    // Show the results
    analysisDF.show()

    // Create a Hive table and load the DataFrame into it
    analysisDF.write.mode("overwrite").saveAsTable("pokemon_analysis")
  }
}
