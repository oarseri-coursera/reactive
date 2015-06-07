/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.{ Actor, Props, ActorRef, ActorSystem }
import akka.testkit.{ TestProbe, ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import scala.concurrent.duration._
import org.scalatest.FunSuiteLike
import org.scalactic.ConversionCheckedTripleEquals

class IntegrationSpec(_system: ActorSystem) extends TestKit(_system)
    with FunSuiteLike
    with Matchers
    with BeforeAndAfterAll
    with ConversionCheckedTripleEquals
    with ImplicitSender
    with Tools {

  import Replica._
  import Replicator._
  import Arbiter._

  def this() = this(ActorSystem("ISpec")) // "IntegrationSpec"

  override def afterAll: Unit = system.shutdown()

  var guardian: ActorRef = _

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */

  test("case1: Totally non-flaky system should handle test sequence of ops") {
    doOpsTest("c1", flakyPersistence = false, flakyReplication = false)
  }

  test("case2: System with flaky persistence should handle test sequence of ops") {
    doOpsTest("c2", flakyPersistence = true, flakyReplication = false)
  }

  // test("case3: System with flaky replication should handle test sequence of ops") {
  //   doOpsTest("c3", flakyPersistence = false, flakyReplication = true)
  // }

  // test("case4: Totally flaky system should handle test sequence of ops") {
  //   doOpsTest("c4", flakyPersistence = true, flakyReplication = true)
  // }

  // Sets up three-node system, optionally with flaky persistence and/or replication,
  // then runs through some basic operations.
  //
  def doOpsTest(caseName: String,
                flakyPersistence: Boolean = false,
                flakyReplication: Boolean = false) = {

    val arbiter = system.actorOf(Props[Arbiter],
      s"${caseName}-arbiter")
    val primary = system.actorOf(
      Replica.props(arbiter, Persistence.props(flaky=flakyPersistence)),
      s"${caseName}-primary")

    // (LOL have to spend some time after creating primary and before
    // creatng secondary, or else you might not be sure that the primary
    // actually joins the cluster first.  Avoided in provided unit tests
    // by using a probe for the arbiter and manually sending Joined<role?
    // messages.)
    val clientP = session(primary)

    val secondary1 = system.actorOf(
      Replica.props(arbiter, Persistence.props(flaky=flakyPersistence)),
      s"${caseName}-secondary1")
    val clientS1 = session(secondary1)

    clientP.getAndVerify("k1")
    clientS1.get("k1") should ===(None)

    clientP.setAcked("k1","100")
    clientP.getAndVerify("k1")
    clientS1.get("k1") should ===(Some("100"))

    clientP.getAndVerify("k2")
    clientS1.get("k2") should ===(None)

    val secondary2 = system.actorOf(
      Replica.props(arbiter, Persistence.props(flaky=flakyPersistence)),
      s"${caseName}-secondary2")
    val clientS2 = session(secondary2)

    Thread.sleep(1000)

    clientS2.get("k1") should ===(Some("100"))
    clientS2.get("k2") should ===(None)

    // clientP.setAcked("k2", "200")
    // clientS1.get("k2") should ===(Some("200"))
    // clientS2.get("k2") should ===(Some("200"))
    // clientP.setAcked("k2", "201")
    // clientS1.get("k2") should ===(Some("201"))
    // clientS2.get("k2") should ===(Some("201"))

  }


}
