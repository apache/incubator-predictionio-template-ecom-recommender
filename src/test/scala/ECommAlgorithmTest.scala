package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import io.prediction.data.storage.BiMap

import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}

@RunWith(classOf[JUnitRunner])
class ECommAlgorithmTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val algorithmParams = new ECommAlgorithmParams(
    appName = "test-app",
    unseenOnly = true,
    seenEvents = List("buy", "view"),
    similarEvents = List("view"),
    rank = 10,
    numIterations = 20,
    lambda = 0.01,
    seed = Some(3)
  )
  val algorithm = new ECommAlgorithm(algorithmParams)

  val userStringIntMap = BiMap(Map(0 -> 0, 1 -> 1))

  val itemStringIntMap = BiMap(Map(0 -> 0, 1 -> 1, 2 -> 2))

  val users = Map(0 -> User(), 1 -> User())


  val i0 = Item(categories = Some(List("c0", "c1")))
  val i1 = Item(categories = None)
  val i2 = Item(categories = Some(List("c0", "c2")))

  val items = Map(
    0 -> i0,
    1 -> i1,
    2 -> i2
  )

  val view = Seq(
    ViewEvent(0, 0, 1000010),
    ViewEvent(0, 1, 1000020),
    ViewEvent(0, 1, 1000020),
    ViewEvent(1, 1, 1000030),
    ViewEvent(1, 2, 1000040)
  )

  val buy = Seq(
    BuyEvent(0, 0, 1000020),
    BuyEvent(0, 1, 1000030),
    BuyEvent(1, 1, 1000040)
  )


  "ECommAlgorithm.genMLlibRating()" should "create RDD[MLlibRating] correctly" in {

    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      items = sc.parallelize(items.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      buyEvents = sc.parallelize(buy.toSeq)
    )

    val mllibRatings = algorithm.genMLlibRating(
      data = preparedData
    )

    val expected = Seq(
      MLlibRating(0, 0, 1),
      MLlibRating(0, 1, 2),
      MLlibRating(1, 1, 1),
      MLlibRating(1, 2, 1)
    )

    mllibRatings.collect should contain theSameElementsAs expected
  }

  "ECommAlgorithm.trainDefault()" should "return popular count for each item" in {
    val preparedData = new PreparedData(
      users = sc.parallelize(users.toSeq),
      items = sc.parallelize(items.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      buyEvents = sc.parallelize(buy.toSeq)
    )

    val popCount = algorithm.trainDefault(
      data = preparedData
    )

    val expected = Map(0 -> 1, 1 -> 2)

    popCount should contain theSameElementsAs expected
  }

  "ECommAlgorithm.predictKnownuser()" should "return top item" in {

    val top = algorithm.predictKnownUser(
      userFeature = Array(1.0, 2.0, 0.5),
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = 0,
        num = 5,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((2, 7.5), (0, 5.0))
    top shouldBe expected
  }

  "ECommAlgorithm.predictDefault()" should "return top item" in {

    val top = algorithm.predictDefault(
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = 0,
        num = 5,
        categories = None,
        whiteList = None,
        blackList = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((1, 4.0), (0, 3.0), (2, 1.0))
    top shouldBe expected
  }

  "ECommAlgorithm.predictSimilar()" should "return top item" in {

    val top = algorithm.predictSimilar(
      recentFeatures = Vector(Array(1.0, 2.0, 0.5), Array(1.0, 0.2, 0.3)),
      productModels = Map(
        0 -> ProductModel(i0, Some(Array(2.0, 1.0, 2.0)), 3),
        1 -> ProductModel(i1, Some(Array(3.0, 0.5, 1.0)), 4),
        2 -> ProductModel(i2, Some(Array(1.0, 3.0, 1.0)), 1)
      ),
      query = Query(
        user = 0,
        num = 5,
        categories = Some(Set("c0")),
        whiteList = None,
        blackList = None),
      whiteList = None,
      blackList = Set()
    )

    val expected = Array((0, 1.605), (2, 1.525))

    top(0)._1 should be (expected(0)._1)
    top(1)._1 should be (expected(1)._1)
    top(0)._2 should be (expected(0)._2 plusOrMinus 0.001)
    top(1)._2 should be (expected(1)._2 plusOrMinus 0.001)
  }
}
