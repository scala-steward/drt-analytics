package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}

object MockData {
  val colNames = Seq("target", "p1", "p2", "index")

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

class DataSetTest extends Specification with AfterAll {
  implicit val session: SparkSession = SparkSession
    .builder
    .appName("DRT Analytics")
    .config("spark.master", "local")
    .getOrCreate()

  override def afterAll(): Unit = session.close()

  val df: DataFrame = MockData.data

  "Given a small DataSet" >> {
    val dataSet = DataSet(df, List(Single("p1"), OneToMany(List("p1", "p2"), "a")))

    "It should provide an indexed version with an incrementing number" >> {
      dataSet.dfIndexed.select("_index").collect().map(_(0)) === Array(0, 1, 2, 3)
    }

    "It should provide all the one to many features" >> {
      dataSet.oneToManyFeatureValues.toSet === Set("a_2.0-3d", "a_2.0-4d", "a_1.0-2d", "a_1.0-1d")
    }

    "It should have a row count matching the number of rows" >> {
      dataSet.numRows === 4
    }
  }
}
