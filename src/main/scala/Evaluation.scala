package org.template.ecommercerecommendation

import io.prediction.controller.Evaluation
import io.prediction.controller.OptionAverageMetric
import io.prediction.controller.AverageMetric
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EngineParamsGenerator
import io.prediction.controller.EngineParams
import io.prediction.controller.MetricEvaluator

import grizzled.slf4j.Logger

// Usage:
// $ pio eval org.template.RecommendationEvaluation \
//   org.template.ParamsList

case class PrecisionAtK(k: Int, scoreThreshold: Double = 1.0)
    extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  require(k > 0, "k must be greater than 0")

  override def header = s"Precision@K (k=$k, threshold=$scoreThreshold)"

  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {
    val positives: Set[String] = a.itemScores.filter(_.score >= scoreThreshold).map(_.item).toSet

    // If there is no positive results, Precision is undefined. We don't consider this case in the
    // metrics, hence we return None.
    if (positives.size == 0) {
      return None
    }

    val tpCount: Int = p.itemScores.take(k).filter(is => positives(is.item)).size

    Some(tpCount.toDouble / math.min(k, positives.size))
  }
}

case class Jaccard(scoreThreshold: Double = 1.0)
    extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  //require(k > 0, "k must be greater than 0")

  @transient lazy val logger = Logger[this.type]

  override def header = s"Jaccard (scoreThreshold=$scoreThreshold)"

  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {
    val positives: Set[String] = a.itemScores.toList.filter(_.score >= scoreThreshold).map(_.item).toSet

    // If there is no positive results, Precision is undefined. We don't consider this case in the
    // metrics, hence we return None.
    if (positives.size == 0) {
      return None
    }

    val aItems = a.itemScores.toList.map(_.item).toSet
    val pItems = p.itemScores.toList.map(_.item).toSet

    // If there are no actual items we don't consider the case in metrics
    if (aItems.size == 0) {
      return None
    } 

    //logger.info(s"Query.user: ${q.user}")
    //logger.info(s"ActualResult size: ${a.itemScores.size}")
    //logger.info(s"PredictedResult size: ${p.itemScores.size}")

    val jVal = jaccardValue(aItems,pItems) 

    logger.info(s"user: ${q.user}, jc: ${jVal}, ars: ${a.itemScores.size}, prs: ${p.itemScores.size}")
    val aa = a.itemScores.toList.map(x=>(x.item+":"+x.score)).mkString(",")
    val bb = p.itemScores.toList.map(x=>(x.item+":"+x.score)).mkString(",")
    //logger.info(s"user: ${q.user}, a: ${aa}, p: ${bb}")

    Some(jVal)     
  }

  def jaccardValue (A: Set[String], B: Set[String]) : Double = {
    return A.intersect(B).size.toDouble / A.union(B).size.toDouble
  }
}

case class PositiveCount(scoreThreshold: Double = 1.0)
    extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  override def header = s"PositiveCount (threshold=$scoreThreshold)"

  def calculate(q: Query, p: PredictedResult, a: ActualResult): Double = {
    a.itemScores.toList.filter(_.score >= scoreThreshold).size
  }
}

object RecommendationEvaluation extends Evaluation {
  engineEvaluator = (
    ECommerceRecommendationEngine(),
    MetricEvaluator(
      metric = Jaccard(scoreThreshold = 1.0),
      otherMetrics = Seq( PositiveCount(scoreThreshold = 1.0) )
    )
  )
}

object ComprehensiveRecommendationEvaluation extends Evaluation {
  val scoreThresholds = Seq(1.0, 10.0, 11.0)

  engineEvaluator = (
    ECommerceRecommendationEngine(),
    MetricEvaluator(
      metric = Jaccard(scoreThreshold = 1.0),
      otherMetrics = (
        (for (r <- scoreThresholds) yield PositiveCount(scoreThreshold = r)) ++
        (for (r <- scoreThresholds) yield Jaccard(scoreThreshold = r))
      )))
}

object RecommendationEvaluation2 extends Evaluation {
  engineEvaluator = (
    ECommerceRecommendationEngine(),
    MetricEvaluator(
      metric = PrecisionAtK(k =10, scoreThreshold = 1.0),
      otherMetrics = Seq( PositiveCount(scoreThreshold = 1.0) )
    )
  )
}

trait BaseEngineParamsList extends EngineParamsGenerator {
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(
      appName = "INVALID_APP_NAME",
      evalParams = Some(DataSourceEvalParams(kFold = 2, queryNum = 5))))
}

object EngineParamsList extends BaseEngineParamsList {
  engineParamsList = for(
    rank <- Seq(5,10,15);
    numIterations <- Seq(20))
    //rank <- Seq(10);
    //numIterations <- Seq(10,20,30))
    yield baseEP.copy(
      algorithmParamsList = Seq(
        ("ecomm", ECommAlgorithmParams("INVALID_APP_NAME", false, List("buy", "view"), List("view"), rank, numIterations, 0.01, Option(3)))) )
}


