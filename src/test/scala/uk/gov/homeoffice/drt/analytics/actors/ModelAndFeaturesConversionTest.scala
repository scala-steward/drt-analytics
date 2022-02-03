package uk.gov.homeoffice.drt.analytics.actors

import org.specs2.mutable.Specification
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, ModelAndFeatures, RegressionModel, TouchdownModelAndFeatures}

class ModelAndFeaturesConversionTest extends Specification {
  "Given a ModelAndFeatures class" >> {
    "I should be able to serialise and deserialise it back to its original form" >> {
      val model = RegressionModel(Seq(1, 2, 3), -1.45)
      val features = FeaturesWithOneToManyValues(List(OneToMany(List("a", "b"), "_a"), Single("c")), IndexedSeq("aa", "bb", "cc"))
      val modelAndFeatures = ModelAndFeatures(model, features, TouchdownModelAndFeatures.targetName, 100, 10.1)

      val serialised = ModelAndFeaturesConversion.modelAndFeaturesToMessage(modelAndFeatures, 0L)

      val deserialised = ModelAndFeaturesConversion.modelAndFeaturesFromMessage(serialised)

      deserialised === modelAndFeatures
    }
  }
}
