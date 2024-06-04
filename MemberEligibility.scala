
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.LazyLogging

object MemberEligibility extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Member Eligibility")
      .master("local[*]")
      .getOrCreate()

    logger.info("Starting Member Eligibility job")

    // Reading input files
    val memberEligibilityPath = "C:\Users\keert\Downloads\member-eligibility\member_eligibility.csv"
    val memberMonthsPath = "C:\Users\keert\Downloads\member-eligibility\member_months.csv"

    val memberEligibilityDF = readCSVFile(spark, memberEligibilityPath)
    val memberMonthsDF = readCSVFile(spark, memberMonthsPath)

    // Calculating total member months
    val totalMemberMonthsDF = calculateTotalMemberMonths(memberEligibilityDF, memberMonthsDF)

    // Saving results to JSON partitioned by memberID
    saveAsJson(totalMemberMonthsDF, "output/total_member_months")

    // Calculating total member months per year
    val memberMonthsPerYearDF = calculateMemberMonthsPerYear(memberEligibilityDF, memberMonthsDF)

    // Saving results to JSON
    saveAsJson(memberMonthsPerYearDF, "output/member_months_per_year")

    logger.info("Job completed successfully")

    // Stop the SparkSession
    spark.stop()
  }

  def readCSVFile(spark: SparkSession, path: String): DataFrame = {
    try {
      logger.info(s"Reading CSV file: $path")
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    } catch {
      case e: Exception =>
        logger.error(s"Error reading CSV file: $path", e)
        throw e
    }
  }

  def calculateTotalMemberMonths(memberEligibilityDF: DataFrame, memberMonthsDF: DataFrame): DataFrame = {
    memberEligibilityDF.join(memberMonthsDF, "memberID")
      .groupBy("memberID", "fullName")
      .agg(count("eligibilityDate").alias("totalMemberMonths"))
  }

  def calculateMemberMonthsPerYear(memberEligibilityDF: DataFrame, memberMonthsDF: DataFrame): DataFrame = {
    memberEligibilityDF.join(memberMonthsDF, "memberID")
      .withColumn("year", year(col("eligibilityDate")))
      .groupBy("memberID", "year")
      .agg(count("eligibilityDate").alias("totalMemberMonths"))
  }

  def saveAsJson(df: DataFrame, outputPath: String): Unit = {
    try {
      logger.info(s"Saving DataFrame to JSON at: $outputPath")
      df.write
        .partitionBy("memberID")
        .json(outputPath)
    } catch {
      case e: Exception =>
        logger.error(s"Error saving DataFrame to JSON at: $outputPath", e)
        throw e
    }
  }
}
