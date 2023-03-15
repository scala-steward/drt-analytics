package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{BestPax, Carrier, Origin}

object MockData {
  val colNames: Seq[String] = Seq("target", "carrier", "origin", "index")

  def data(implicit session: SparkSession): DataFrame = {
    import session.implicits._

    List(
      (1d, 1d, "1d", "1"),
      (2d, 1d, "2d", "2"),
      (2d, 2d, "3d", "3"),
      (4d, 2d, "4d", "4"),
    ).toDF(colNames: _*)
  }
}

class DataSetTest extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  implicit val session: SparkSession = SparkSession
    .builder
    .config("spark.master", "local")
    .getOrCreate()

  override def afterAll(): Unit = session.close()

  val df: DataFrame = MockData.data

  "A small DataSet" should {
    val dataSet = DataSet(df, List(Single(BestPax), OneToMany(List(Carrier, Origin), "a")))

    "Provide an indexed version with an incrementing number" in {
      dataSet.dfIndexed.select("_index").collect().map(_(0)) === Array(0, 1, 2, 3)
    }

    "Provide all the one to many features" in {
      dataSet.oneToManyFeatureValues.toSet === Set("a_2.0-3d", "a_2.0-4d", "a_1.0-2d", "a_1.0-1d")
    }

    "Have a row count matching the number of rows" in {
      dataSet.numRows === 4
    }
  }
}
