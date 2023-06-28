package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.prediction.FeaturesWithOneToManyValues
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.{BestPax, Carrier}

class FeatureVectorsSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  implicit val session: SparkSession = SparkSession
    .builder
    .config("spark.master", "local")
    .getOrCreate()

  import session.implicits._

  override def afterAll(): Unit = session.close()


  "Given a Row from a DataFrame and a FeaturesWithOneToManyValues, FeatureVectors" should {
    val colNames = Seq("label", "bestPax", "carrier", "origin", "bestPax", "index")
    val row = List((1d, 5d, "1d", "2d", 3d, "1")).toDF(colNames: _*).collect().head

    "Give me the value of the one single feature" in {
      val features = FeaturesWithOneToManyValues(List(BestPax), oneToManyValues = IndexedSeq())
      FeatureVectors.featuresVectorForRow(row, features) === ml.linalg.Vectors.dense(Array(5d))
    }

    "Give me an empty vector for an empty set of features" in {
      val features = FeaturesWithOneToManyValues(List(Carrier), oneToManyValues = IndexedSeq())
      FeatureVectors.featuresVectorForRow(row, features) === ml.linalg.Vectors.dense(Array[Double]())
    }

    "Give me (1) for a matched feature value" in {
      val features = FeaturesWithOneToManyValues(List(Carrier), oneToManyValues = IndexedSeq("a_1d"))
      FeatureVectors.featuresVectorForRow(row, features) === ml.linalg.Vectors.dense(Array(1d))
    }

    "Give me (0, 1) where we match the second of 2 possible values" in {
      val features = FeaturesWithOneToManyValues(List(Carrier), oneToManyValues = IndexedSeq("a_xx", "a_1d"))
      FeatureVectors.featuresVectorForRow(row, features) === ml.linalg.Vectors.dense(Array(0d, 1d))
    }

    "Give me (1, 0) where we match the first of 2 possible values" in {
      val features = FeaturesWithOneToManyValues(List(Carrier), oneToManyValues = IndexedSeq("a_1d", "a_xx"))
      FeatureVectors.featuresVectorForRow(row, features) === ml.linalg.Vectors.dense(Array(1d, 0d))
    }

    "Give me (1, 0, 5d) where the single feature value is 5 and the one to many matches the first of 2 values" in {
      val features = FeaturesWithOneToManyValues(List(BestPax, Carrier), oneToManyValues = IndexedSeq("a_1d", "a_xx"))
      FeatureVectors.featuresVectorForRow(row, features) === ml.linalg.Vectors.dense(Array(1d, 0d, 5d))
    }

    "Give me (1, 0, 5d, 3d) where the one to many matches the first of 2 values, followed by the two singles" in {
      val features = FeaturesWithOneToManyValues(List(BestPax, BestPax, Carrier), oneToManyValues = IndexedSeq("a_1d", "a_xx"))
      FeatureVectors.featuresVectorForRow(row, features) === ml.linalg.Vectors.dense(Array(1d, 0d, 5d, 3d))
    }
  }
}
