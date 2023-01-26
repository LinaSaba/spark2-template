package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }
  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demDF = spark.read.json("data/input/demographie_par_commune.json")
    val population = demDF.select($"Population")
      .agg(sum($"Population")
        .as("FrPopulation"))
    population.show

    val pop_dep = demDF.select($"Population", $"Departement")

    val high_pop_dep= pop_dep.groupBy($"Departement")
      .sum()
      .sort(desc("sum(Population)"))
    high_pop_dep.show(10)

    val depDF = spark.read.csv("data/input/departements.txt")
      .select($"_c0".as(alias="name"), $"_c1".as(alias="code"))

    high_pop_dep.join(depDF, depDF("code") ===  high_pop_dep("Departement")).show(10)
  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s07DF = spark.read.option("header", false)
      .option("sep", "\t")
      .csv("data/input/sample_07")
      .select($"_c0".as(alias = "Code"), $"_c1".as(alias = "Job"), $"_c2".as(alias = "Count"), $"_c3".as(alias = "Salary07"))

    val s08DF = spark.read.option("header", false)
      .option("sep", "\t")
      .csv("data/input/sample_08")
      .select($"_c0".as(alias = "Code"), $"_c1".as(alias = "Job"), $"_c2".as(alias = "Count"), $"_c3".as(alias = "Salary08"))

    val topSalaries07 = s07DF.select($"Salary07")
      .where($"Salary07">100000)
      .sort(desc("Salary07"))
    topSalaries07.show(10)

    val growth = s07DF.join(s08DF, s07DF("Code") === s08DF("Code"))
      .select(s08DF("Job"),(s08DF("Salary08")-s07DF("Salary07"))as("growth"))
      .where(s07DF("Salary07")<s08DF("Salary08"))
      .sort(desc("growth"))
    growth.show(10)

    val jobloss = s07DF.join(s08DF, s07DF("Code") === s08DF("Code"))
      .select(s08DF("Job"), (s08DF("Count") - s07DF("Count")) as ("jobloss"))
      .where(s07DF("Salary07") > 100000 and s08DF("Salary08") > 100000)
      .sort(desc("jobloss"))
    jobloss.show(10)
  }

  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    val DifficultyLevels = toursDF.agg(countDistinct("tourDifficulty") as "nb of unique levels of difficulties")
    DifficultyLevels.show

    val tourPrice = toursDF.select(min("tourPrice") as ("mintourPrice"),
      max("tourPrice") as ("maxtourPrice"),
      avg("tourPrice") as ("avgtourPrice"))
    tourPrice.show

    val tourPriceD = toursDF.groupBy("tourDifficulty")
      .agg(min("tourPrice") as ("mintourPrice"),
      max("tourPrice") as ("maxtourPrice"),
      avg("tourPrice") as ("avgtourPrice"))
    tourPriceD.show

    val tourDuration = toursDF.columns(4)
    val tourPriceDurationD = toursDF.groupBy("tourDifficulty")
      .agg(min("tourPrice") as ("mintourPrice"),
      max("tourPrice") as ("maxtourPrice"),
      avg("tourPrice") as ("avgtourPrice"),
      min(tourDuration) as ("mintourDuration"),
      max(tourDuration) as ("maxtourDuration"),
      avg(tourDuration) as ("avgtourDuration")  )
    tourPriceDurationD.show

    val top10Tags = toursDF.select($"tourName", explode($"tourTags") as "tags")
      .groupBy("tags")
      .count()
      .orderBy($"count".desc)
    top10Tags.show(10)

    val relationShip = toursDF.select(explode($"tourTags") as "tags", $"tourDifficulty")
      .groupBy("tags", "tourDifficulty")
      .count()
      .orderBy($"count".desc)
    relationShip.show(10)

    val question7 = toursDF.select(explode($"tourTags") as "tags", $"tourDifficulty", $"tourPrice")
      .groupBy("tags", "tourDifficulty")
      .agg(min("tourPrice") as ("mintourPrice"),
        max("tourPrice") as ("maxtourPrice"),
        avg("tourPrice") as ("avgtourPrice"))
      .orderBy($"avgtourPrice".desc)
    question7.show(10)
  }
}
