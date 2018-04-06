package Scala

import Irisa.Enssat.Rennes1.thesis.sparkSQL.{Pareto, historicData}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable
/**
  * Created by letrungdung on 07/03/2018.
  */
case class MultipleCostBasedJoinReorder(confSQL: SQLConf) extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!confSQL.cboEnabled || !confSQL.joinReorderEnabled) {
      plan
    } else {
      val result = plan transformDown {
        // Start reordering with a joinable item, which is an InnerLike join with conditions.
        case j @ Join(_, _, _: InnerLike, Some(cond)) =>
          reorder(j, j.output)
        case p @ Project(projectList, Join(_, _, _: InnerLike, Some(cond)))
          if projectList.forall(_.isInstanceOf[Attribute]) =>
          reorder(p, p.output)
        //case f @ Pro
      }
      // After reordering is finished, convert OrderedJoin back to Join
      result transformDown {
        case oj: OrderedJoin => oj.join
      }
    }
  }

  private def reorder(plan: LogicalPlan, output: Seq[Attribute]): LogicalPlan = {
    val (items, conditions) = extractInnerJoins(plan)
    val test = items.forall(_.stats(confSQL).rowCount.isDefined)
    //println("----------------test: " + test)
    /*
    if(test){
      items.foreach(plan =>
        //println("yes"+plan.numberedTreeString)
      )
    }
    else{
      items.foreach(plan =>
        //println("no+"+plan.numberedTreeString)
      )
    }
    */
    //println("-------end of print item---------test: ")
    // TODO: Compute the set of star-joins and use them in the join enumeration
    // algorithm to prune un-optimal plan choices.
    val result =
    // Do reordering if the number of items is appropriate and join conditions exist.
    // We also need to check if costs of all items can be evaluated.
    if (items.size > 2
      && items.size <= confSQL.joinReorderDPThreshold
      && conditions.nonEmpty
      && items.forall(_.stats(confSQL).rowCount.isDefined)
    ) {
      MultipleJoinReorderDP.search(confSQL, items, conditions, output)
    } else {
      plan
    }
    // Set consecutive join nodes ordered.
    replaceWithOrderedJoin(result)
  }

  /**
    * Extracts items of consecutive inner joins and join conditions.
    * This method works for bushy trees and left/right deep trees.
    */
  private def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], Set[Expression]) = {
    plan match {
      case Join(left, right, _: InnerLike, Some(cond)) =>
        val (leftPlans, leftConditions) = extractInnerJoins(left)
        val (rightPlans, rightConditions) = extractInnerJoins(right)
        (leftPlans ++ rightPlans, splitConjunctivePredicates(cond).toSet ++
          leftConditions ++ rightConditions)
      case Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond)))
        if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(j)
      case _ =>
        (Seq(plan), Set())
    }
  }

  private def replaceWithOrderedJoin(plan: LogicalPlan): LogicalPlan = plan match {
    case j @ Join(left, right, _: InnerLike, Some(cond)) =>
      val replacedLeft = replaceWithOrderedJoin(left)
      val replacedRight = replaceWithOrderedJoin(right)
      OrderedJoin(j.copy(left = replacedLeft, right = replacedRight))
    case p @ Project(projectList, j @ Join(_, _, _: InnerLike, Some(cond))) =>
      p.copy(child = replaceWithOrderedJoin(j))
    case _ =>
      plan
  }

  /** This is a wrapper class for a join node that has been ordered. */
  private case class OrderedJoin(join: Join) extends BinaryNode {
    override def left: LogicalPlan = join.left
    override def right: LogicalPlan = join.right
    override def output: Seq[Attribute] = join.output
  }
}

/**
  * Reorder the joins using a dynamic programming algorithm. This implementation is based on the
  * paper: Access Path Selection in a Relational Database Management System.
  * http://www.inf.ed.ac.uk/teaching/courses/adbs/AccessPath.pdf
  *
  * First we put all items (basic joined nodes) into level 0, then we build all two-way joins
  * at level 1 from plans at level 0 (single items), then build all 3-way joins from plans
  * at previous levels (two-way joins and single items), then 4-way joins ... etc, until we
  * build all n-way joins and pick the best plan among them.
  *
  * When building m-way joins, we only keep the best plan (with the lowest cost) for the same set
  * of m items. E.g., for 3-way joins, we keep only the best plan for items {A, B, C} among
  * plans (A J B) J C, (A J C) J B and (B J C) J A.
  * We also prune cartesian product candidates when building a new plan if there exists no join
  * condition involving references from both left and right. This pruning strategy significantly
  * reduces the search space.
  * E.g., given A J B J C J D with join conditions A.k1 = B.k1 and B.k2 = C.k2 and C.k3 = D.k3,
  * plans maintained for each level are as follows:
  * level 0: p({A}), p({B}), p({C}), p({D})
  * level 1: p({A, B}), p({B, C}), p({C, D})
  * level 2: p({A, B, C}), p({B, C, D})
  * level 3: p({A, B, C, D})
  * where p({A, B, C, D}) is the final output plan.
  *
  * For cost evaluation, since physical costs for operators are not available currently, we use
  * cardinalities and sizes to compute costs.
  */
object MultipleJoinReorderDP extends PredicateHelper with Logging {

  def search(
              conf: SQLConf,
              items: Seq[LogicalPlan],
              conditions: Set[Expression],
              output: Seq[Attribute]): LogicalPlan = {

    val startTime = System.nanoTime()
    // Level i maintains all found plans for i + 1 items.
    // Create the initial plans: each plan is a single item with zero cost.
    val itemIndex = items.zipWithIndex
    // found Plans is all the possible logic plans in Map for a plan //
    val foundPlans = mutable.Buffer[JoinPlanMap](itemIndex.map {
      case (item, id) => Set(id) -> JoinPlan(Set(id), item, Set(), MultipleCost(0, 0, 0, 0))
    }.toMap)

    // Build plans for next levels until the last level has only one plan. This plan contains
    // all items that can be joined, so there's no need to continue.
    val topOutputSet = AttributeSet(output)
    while (foundPlans.size <= items.length && foundPlans.last.size > 1) {
      conf.setConfString("items.length",items.length.toString)
      //println("items.length.toString:=" + conf.getConfString("items.length").toInt)
      // Build plans for the next level.
      foundPlans += searchLevel(foundPlans, conf, conditions, topOutputSet)
    }

    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logDebug(s"Join reordering finished. Duration: $durationInMs ms, number of items: " +
      s"${items.length}, number of plans in memo: ${foundPlans.map(_.size).sum}")

    //bestPlan(foundPlans, items.length)
    println(s"Join reordering finished. Duration: $durationInMs ms, number of items: " +
      s"${items.length}, number of plans in memo: ${foundPlans.map(_.size).sum}")
    // The last level must have one and only one plan, because all items are joinable.
    assert(foundPlans.size == items.length && foundPlans.last.size == 1)
    foundPlans.last.head._2.plan match {
      case p @ Project(projectList, j: Join) if projectList != output =>
        assert(topOutputSet == p.outputSet)
        // Keep the same order of final output attributes.
        p.copy(projectList = output)
      case finalPlan =>
        finalPlan
    }
  }
  def bestPlan (foundPlan: mutable.Buffer[JoinPlanMap], length: Int): Unit ={
    println("**********************************************************")
    val mapPlan = foundPlan.last
    val values = mapPlan.values
    val keys = mapPlan.keys
    for (key <- keys){
      println(key)
    }
    for (value <- values){
      val plan = value.plan
      val cost = value.planCost
      val card = cost.card
      val size = cost.size
      println(plan)
      println(card)
      println(size)
    }
    println("**********************************************************")
  }
  /** Find all possible plans at the next level, based on existing levels. */
  private def searchLevel(
                           existingLevels: Seq[JoinPlanMap],
                           conf: SQLConf,
                           conditions: Set[Expression],
                           topOutput: AttributeSet): JoinPlanMap = {

    val nextLevel = mutable.Map.empty[Set[Int], JoinPlan]
    val dungLevel = mutable.Map.empty[List[Int], JoinPlan]
    var k = 0
    val lev = existingLevels.length - 1
    // Build plans for the next level from plans at level k (one side of the join) and level
    // lev - k (the other side of the join).
    // For the lower level k, we only need to search from 0 to lev - k, because when building
    // a join from A and B, both A J B and B J A are handled.
    while (k <= lev - k) {
      val oneSideCandidates = existingLevels(k).values.toSeq
      for (i <- oneSideCandidates.indices) {
        val oneSidePlan = oneSideCandidates(i)
        val otherSideCandidates = if (k == lev - k) {
          // Both sides of a join are at the same level, no need to repeat for previous ones.
          oneSideCandidates.drop(i)
        } else {
          existingLevels(lev - k).values.toSeq
        }

        otherSideCandidates.foreach { otherSidePlan =>
          buildJoin(oneSidePlan, otherSidePlan, conf, conditions, topOutput) match {
            case Some(newJoinPlan) =>
              // Check if it's the first plan for the item set, or it's a better plan than
              // the existing one due to lower cost.
              val existingPlan = nextLevel.get(newJoinPlan.itemIds)
              if (existingPlan.isEmpty || newJoinPlan.betterThan(existingPlan.get, conf)) {
                nextLevel.update(newJoinPlan.itemIds, newJoinPlan)
                dungLevel.update(newJoinPlan.itemIds.toList, newJoinPlan)
                historicData.setupFolder(conf.getConfString("idQuery"),newJoinPlan.itemIds.toList.toString())
              }
              else {
                if (existingPlan.get.betterThan(newJoinPlan, conf)) {
                  if (conf.getConfString("items.length").toInt == newJoinPlan.itemIds.size){
                    nextLevel.update(newJoinPlan.itemIds, newJoinPlan)
                    dungLevel.update(newJoinPlan.itemIds.toList, newJoinPlan)
                    historicData.setupFolder(conf.getConfString("idQuery"),newJoinPlan.itemIds.toList.toString())
                  }
                }
                else{
                  nextLevel.update(newJoinPlan.itemIds, newJoinPlan)
                  dungLevel.update(newJoinPlan.itemIds.toList, newJoinPlan)
                  historicData.setupFolder(conf.getConfString("idQuery"),newJoinPlan.itemIds.toList.toString())
                }
                dungLevel.update(newJoinPlan.itemIds.toList, newJoinPlan)
              }
            case None =>
          }
        }
      }
      k += 1
    }
    val listMapLogicalPlan = dungLevel.result().toList
    //TestCostBasedJoinReorder.
    takeListPlan(listMapLogicalPlan)
    historicData.storeIdQuery(conf.getConfString("idQuery"))
    nextLevel.toMap
  }

  val allPlanMap = mutable.Map.empty[List[Int], JoinPlan]
  def takeListPlan(listMapLogicalPlan: List[(List[Int], JoinPlan)]):Unit = {
    for (temp <- listMapLogicalPlan){
      val listInt = temp._1
      val listJoin = temp._2
      allPlanMap.update(listInt,listJoin)
    }
    takeLogicalPlan()
  }
  val allPlanList = List.empty[LogicalPlan]

  def takeLogicalPlan(): Unit ={
    for(temp<-allPlanMap){
      Pareto.addLogicalPlan(temp._2.plan)
      Pareto.addCostPlan(temp._2.planCost)
      Pareto.addSetPlan(temp._1)
    }
    Pareto.filterPlans()
  }
  /**
    * Builds a new JoinPlan when both conditions hold:
    * - the sets of items contained in left and right sides do not overlap.
    * - there exists at least one join condition involving references from both sides.
    * @param oneJoinPlan One side JoinPlan for building a new JoinPlan.
    * @param otherJoinPlan The other side JoinPlan for building a new join node.
    * @param conf SQLConf for statistics computation.
    * @param conditions The overall set of join conditions.
    * @param topOutput The output attributes of the final plan.
    * @return Builds and returns a new JoinPlan if both conditions hold. Otherwise, returns None.
    */
  private def buildJoin(
                         oneJoinPlan: JoinPlan,
                         otherJoinPlan: JoinPlan,
                         conf: SQLConf,
                         conditions: Set[Expression],
                         topOutput: AttributeSet): Option[JoinPlan] = {

    if (oneJoinPlan.itemIds.intersect(otherJoinPlan.itemIds).nonEmpty) {
      // Should not join two overlapping item sets.
      return None
    }

    val onePlan = oneJoinPlan.plan
    val otherPlan = otherJoinPlan.plan
    val joinConds = conditions
      .filterNot(l => canEvaluate(l, onePlan))
      .filterNot(r => canEvaluate(r, otherPlan))
      .filter(e => e.references.subsetOf(onePlan.outputSet ++ otherPlan.outputSet))
    if (joinConds.isEmpty) {
      // Cartesian product is very expensive, so we exclude them from candidate plans.
      // This also significantly reduces the search space.
      return None
    }

    // Put the deeper side on the left, tend to build a left-deep tree.
    val (left, right) = if (oneJoinPlan.itemIds.size >= otherJoinPlan.itemIds.size) {
      (onePlan, otherPlan)
    } else {
      (otherPlan, onePlan)
    }
    val newJoin = Join(left, right, Inner, joinConds.reduceOption(And))
    val collectedJoinConds = joinConds ++ oneJoinPlan.joinConds ++ otherJoinPlan.joinConds
    val remainingConds = conditions -- collectedJoinConds
    val neededAttr = AttributeSet(remainingConds.flatMap(_.references)) ++ topOutput
    val neededFromNewJoin = newJoin.output.filter(neededAttr.contains)
    val newPlan =
      if ((newJoin.outputSet -- neededFromNewJoin).nonEmpty) {
        Project(neededFromNewJoin, newJoin)
      } else {
        newJoin
      }

    val itemIds = oneJoinPlan.itemIds.union(otherJoinPlan.itemIds)
    // Now the root node of onePlan/otherPlan becomes an intermediate join (if it's a non-leaf
    // item), so the cost of the new join should also include its own cost.
    val newPlanCost = oneJoinPlan.planCost +
      oneJoinPlan.rootCost(conf) +
      otherJoinPlan.planCost +
      otherJoinPlan.rootCost(conf)
    Some(JoinPlan(itemIds, newPlan, collectedJoinConds, newPlanCost))
  }

  /** Map[set of item ids, join plan for these items] */
  type JoinPlanMap = Map[Set[Int], JoinPlan]

  /**
    * Partial join order in a specific level.
    *
    * @param itemIds Set of item ids participating in this partial plan.
    * @param plan The plan tree with the lowest cost for these items found so far.
    * @param joinConds Join conditions included in the plan.
    * @param planCost The cost of this plan tree is the sum of costs of all intermediate joins.
    */
  case class JoinPlan(
                       itemIds: Set[Int],
                       plan: LogicalPlan,
                       joinConds: Set[Expression],
                       planCost: MultipleCost) {

    /** Get the cost of the root node of this plan tree. */
    def rootCost(conf: SQLConf): MultipleCost = {
      //val allConfs = conf.getConfString("spark.master")
      //println(allConfs)
      if (itemIds.size > 1) {
        val rootStats = plan.stats(conf)
        println(conf.getConfString("items.length").toInt + " and "+ itemIds.size)
        println("rootStats.sizeInBytes" + rootStats.rowCount.get + " and "+ "rootStats.sizeInBytes" +rootStats.sizeInBytes)
        MultipleCost(rootStats.rowCount.get,
            rootStats.sizeInBytes,
            0,
            0)
      } else {
        // If the plan is a leaf item, it has zero cost.
        MultipleCost(0, 0, 0, 0)
      }
    }

    def betterThan(other: JoinPlan, conf: SQLConf): Boolean = {
      if (other.planCost.card == 0 || other.planCost.size == 0) {
        false
      } else {
        val relativeRows = BigDecimal(this.planCost.card) / BigDecimal(other.planCost.card)
        val relativeSize = BigDecimal(this.planCost.size) / BigDecimal(other.planCost.size)
        //////////////

        if ((this.planCost.card < other.planCost.card)&&(this.planCost.size < other.planCost.size)){
          true
        }
        else {
          false
        }
        /*
        relativeRows * conf.joinReorderCardWeight +
        relativeSize * (1 - conf.joinReorderCardWeight) < 1
        */
      }
    }

  }
}

  /*
  * This class defines the cost model for a plan.
  * @param card Cardinality (number of rows).
  * @param size Size in bytes.
  */

/*
case class MultipleCost(card: BigInt, size: BigInt) {
  def +(other: MultipleCost): MultipleCost = MultipleCost(this.card + other.card, this.size + other.size)
}*/
case class MultipleCost(card: BigInt, size: BigInt, executeTime: Double, moneytary: BigInt) {
  def +(other: MultipleCost): MultipleCost = MultipleCost(this.card + other.card, this.size + other.size,
    this.executeTime + other.executeTime, this.moneytary + other.moneytary)
}



