package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.prediction.FeatureType.{FeatureType, OneToMany, Single}

class BasicLearning extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  implicit val session: SparkSession = SparkSession
    .builder
    .appName("DRT Analytics")
    .config("spark.master", "local")
    .getOrCreate()

  import session.implicits._

  override def afterAll(): Unit = session.close()

  "A Dataset" ignore {
    "be able to train a single variate model and predict values accurately" in {
      val colNames = Seq("target", "p1", "index")

      val data: DataFrame = List(
        (1d, 1d, "1"),
        (2d, 2d, "2"),
        (0d, 3d, "3"),
        (0d, 4d, "4"),
      ).toDF(colNames: _*)

      val featureSpecs = List(Single("p1"))

      trainAndPredict(data, featureSpecs).values.map(_.round) should ===(Array(3d, 4d))
    }

    "train a model with 5 coefficients when given 1 single value feature and a one to many feature with 4 values (p2)" in {
      val colNames = Seq("target", "p1", "p2", "index")

      val data: DataFrame = List(
        (1d, 1d, "1d", "1"),
        (2d, 1d, "2d", "2"),
        (2d, 2d, "3d", "3"),
        (4d, 2d, "4d", "4"),
      ).toDF(colNames: _*)

      val featureSpecs = List(OneToMany(List("p2"), "f1"), Single("p1"))

      DataSet(data, featureSpecs).trainModel("target", 100).coefficients.size should ===(5)
    }
  }

  private def trainAndPredict(data: DataFrame, featureSpecs: List[FeatureType]): Map[String, Double] = {
    val dataSet = DataSet(data, featureSpecs)

    val model = dataSet.trainModel("target", 50)

    dataSet
      .predict("target", 50, model)
      .collect()
      .map { row =>
        row.getAs[String]("index") -> row.getAs[Double]("prediction")
      }.toMap
  }
}
