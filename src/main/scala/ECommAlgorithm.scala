package org.template.ecommercerecommendation

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap
import io.prediction.data.storage.Event
import io.prediction.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

case class ECommAlgorithmParams(
  appName: String,
  unseenOnly: Boolean,
  seenEvents: List[String],
  similarEvents: List[String],
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]
) extends Params


case class ProductModel(
  item: Item,
  features: Option[Array[Double]], // features by ALS
  count: Int // popular count for default score
)

class ECommModel(
  val rank: Int,
  val userFeatures: Map[Int, Array[Double]],
  val productModels: Map[Int, ProductModel]
) extends Serializable {

//  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
    s" userFeatures: [${userFeatures.size}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productModels: [${productModels.size}]" +
    s"(${productModels.take(2).toList}...)"
  }
}

class ECommAlgorithm(val ap: ECommAlgorithmParams)
  extends P2LAlgorithm[PreparedData, ECommModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): ECommModel = {
    require(!data.viewEvents.take(1).isEmpty,
      s"viewEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.users.take(1).isEmpty,
      s"users in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")

    val mllibRatings: RDD[MLlibRating] = genMLlibRating(data)

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
      " Please check if your events contain valid user and item ID.")
      
    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)
    
    // use ALS to train feature vectors
    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed
    )

    val userFeatures = m.userFeatures.collectAsMap.toMap

    val items = data.items

    // join item with the trained productFeatures
    val productFeatures: Map[Int, (Item, Option[Array[Double]])] =
      items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    val popularCount = trainDefault(data)

    val productModels: Map[Int, ProductModel] = productFeatures
      .map { case (index, (item, features)) =>
        val pm = ProductModel(
          item = item,
          features = features,
          // NOTE: use getOrElse because popularCount may not contain all items.
          count = popularCount.getOrElse(index, 0)
        )
        (index, pm)
      }

    new ECommModel(
      rank = m.rank,
      userFeatures = userFeatures,
      productModels = productModels
    )
  }

  /** Generate MLlibRating from PreparedData.
    * You may customize this function if use different events or different aggregation method
    */
  def genMLlibRating(data: PreparedData): RDD[MLlibRating] = {

	logger.info(s"Getting mllib ratings from viewEvents ${data.viewEvents.partitions.size}")


    val mllibRatings = data.viewEvents
      .map { r =>
        // get user and item Int IDs for MLlib
        val uindex = r.user
        val iindex = r.item

        ((uindex, iindex), 1)
      }
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair
      .map { case ((u, i), v) =>
        // MLlibRating requires integer index for user and item
        MLlibRating(u, i, v)
      }
      //.cache()
      //.persist(StorageLevel.MEMORY_AND_DISK_SER)
      
    mllibRatings
  }

  /** Train default model.
    * You may customize this function if use different events or
    * need different ways to count "popular" score or return default score for item.
    */
  def trainDefault(
    data: PreparedData): Map[Int, Int] = {
    // count number of buys
    // (item index, count)
    val buyCountsRDD: RDD[(Int, Int)] = data.buyEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = r.user
        val iindex = r.item

        (uindex, iindex, 1)
      }
      .map { case (u, i, v) => (i, 1) } // key is item
      .reduceByKey{ case (a, b) => a + b } // count number of items occurrence

    buyCountsRDD.collectAsMap.toMap
  }

  def predict(model: ECommModel, query: Query): PredictedResult = {

    val userFeatures = model.userFeatures
    val productModels = model.productModels    
    
    val whiteList: Option[Set[Int]] = query.whiteList

    val finalBlackList: Set[Int] = genBlackList(query = query)
    
    val userFeature: Option[Array[Double]] = userFeatures.get(query.user);
    
    val topScores: Array[(Int, Double)] = if (userFeature.isDefined) {
      // the user has feature vector
      predictKnownUser(
        userFeature = userFeature.get,
        productModels = productModels,
        query = query,
        whiteList = whiteList,
        blackList = finalBlackList
      )
    } else {
      // the user doesn't have feature vector.
      // For example, new user is created after model is trained.
      logger.info(s"No userFeature found for user ${query.user}.")

      // check if the user has recent events on some items
      val recentList: Set[Int] = getRecentItems(query)

      val recentFeatures: Vector[Array[Double]] = recentList.toVector
        // productModels may not contain the requested item
        .map { i =>
          productModels.get(i).flatMap { pm => pm.features }
        }.flatten

      if (recentFeatures.isEmpty) {
        logger.info(s"No features vector for recent items ${recentList}.")
        predictDefault(
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList
        )
      } else {
        predictSimilar(
          recentFeatures = recentFeatures,
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList
        )
      }
    }

    val itemScores = topScores.map { case (i, s) =>
      new ItemScore(
        item = i,
        score = s
      )
    }

    new PredictedResult(itemScores)
  }

  /** Generate final blackList based on other constraints */
  def genBlackList(query: Query): Set[Int] = {
    // if unseenOnly is True, get all seen items
    val seenItems: Set[Int] = if (ap.unseenOnly) {

      // get all user item events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user.toString(),
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some("item")),
          // set time limit to avoid super long DB access
          timeout = Duration(200, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get.toInt
        } catch {
          case e => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[Int]()
    }

    // get the latest constraint unavailableItems $set event
    val unavailableItems: Set[Int] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Set[Int]]("items")
      } else {
        Set[Int]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableItems event." +
          s" Empty list is used. ${e}")
        Set[Int]()
      case e: Exception =>
        logger.error(s"Error when read set unavailableItems event: ${e}")
        throw e
    }

    // combine query's blackList,seenItems and unavailableItems
    // into final blackList.
    query.blackList.getOrElse(Set[Int]()) ++ seenItems ++ unavailableItems
  }

  /** Get recent events of the user on items for recommending similar items */
  def getRecentItems(query: Query): Set[Int] = {
    // get latest 10 user view item events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user.toString(),
        eventNames = Some(ap.similarEvents),
        targetEntityType = Some(Some("item")),
        limit = Some(10),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[Int] = recentEvents.map { event =>
      try {
        event.targetEntityId.get.toInt
      } catch {
        case e => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }

  /** Prediction for user with known feature vector */
  def predictKnownUser(
    userFeature: Array[Double],
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        // NOTE: features must be defined, so can call .get
        val s = dotProduct(userFeature, pm.features.get)
        // may customize here to further adjust score
        (i, s)
      }
      .filter(_._2 > 0) // only keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Default prediction when know nothing about the user */
  def predictDefault(
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert back to sequential collection
      .filter { case (i, pm) =>
        isCandidateItem(
          i = i,
          item = pm.item,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        // may customize here to further adjust score
        (i, pm.count.toDouble)
      }
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Return top similar items based on items user recently has action on */
  def predictSimilar(
    recentFeatures: Vector[Array[Double]],
    productModels: Map[Int, ProductModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
        pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, pm) =>
        val s = recentFeatures.map{ rf =>
          // pm.features must be defined because of filter logic above
          cosine(rf, pm.features.get)
        }.reduce(_ + _)
        // may customize here to further adjust score
        (i, s)
      }
      .filter(_._2 > 0) // keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  private
  def getTopN[T](s: Iterable[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private
  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private
  def isCandidateItem(
    i: Int,
    item: Item,
    categories: Option[Set[String]],
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Boolean = {
    // can add other custom filtering here
    whiteList.map(_.contains(i)).getOrElse(true) &&
    !blackList.contains(i) &&
    // filter categories
    categories.map { cat =>
      item.categories.map { itemCat =>
        // keep this item if has ovelap categories with the query
        !(itemCat.toSet.intersect(cat).isEmpty)
      }.getOrElse(false) // discard this item if it has no categories
    }.getOrElse(true)

  }

}
