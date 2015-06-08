/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }
import akka.testkit.{ TestProbe, ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import scala.util.Random
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

  object Secondary {
    // Not taking any defensive measures to prevent this being used as a primary
    // replica; up to caller to make sure arbiter does the right thing in system.
    class FlakySecondary(val arbiter: ActorRef,
                         val flakyPersistence: Boolean,
                         val nameRoot: String) extends Actor with ActorLogging {

      // Create a captive real secondary actor.  We have to impersonate the arbiter
      // or else actualSecondary goes behind our back and registers with the arbiter
      // directly, getting all the future messages generated by primary and its
      // replicas without allowing us to interfere, lol.
      val actualSecondary: ActorRef = system.actorOf(
        Replica.props(self, Persistence.props(flaky=flakyPersistence)),
        s"${nameRoot}-actual")

      def receive = {
        // Impersonate arbiter.
        case Join =>
          arbiter ! Join
        case JoinedSecondary =>
          actualSecondary ! JoinedSecondary
        // Don't flake out on Get queries, we want to be able to test!
        case g: Get =>
          actualSecondary forward g
        // Okay, now start dropping messages at random.
        case msg =>
          if (Random.nextBoolean()) {
            actualSecondary forward msg
          } else {
            log.debug(s"Flaky secondary dropping message ${msg}.")
          }
      }
    }
    object FlakySecondary {
      def props(arbiter: ActorRef, flakyPersistence: Boolean, nameRoot: String) =
        Props(new FlakySecondary(arbiter, flakyPersistence, nameRoot))
    }

    def gen(arbiter: ActorRef,
            flakyPersistence: Boolean,
            flakyReplication: Boolean,
            name: String): ActorRef = {
      if (flakyReplication) {
        system.actorOf(
          FlakySecondary.props(arbiter, flakyPersistence, name), s"${name}-shim")
      } else {
        system.actorOf(
          Replica.props(arbiter, Persistence.props(flaky=flakyPersistence)), name)
      }
    }
  }

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

  test("case3: System with flaky replication should handle test sequence of ops") {
    doOpsTest("c3", flakyPersistence = false, flakyReplication = true)
  }

  test("case4: Totally flaky system should handle test sequence of ops") {
    doOpsTest("c4", flakyPersistence = true, flakyReplication = true)
  }

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
    // creatng secondary, or else it is pretty uncertain that the primary
    // actually joins the cluster first.  Provided unit tests avoided this
    // by using a probe for the arbiter and manually sending Joined<role>
    // messages.)
    val clientP = session(primary)

    val secondary1 = Secondary.gen(arbiter, flakyPersistence, flakyReplication,
      s"${caseName}-secondary1")
    val clientS1 = session(secondary1)

    clientP.getAndVerify("k1")
    clientS1.get("k1") should ===(None)

    clientP.setAcked("k1","100")
    clientP.getAndVerify("k1")
    clientS1.get("k1") should ===(Some("100"))

    clientP.getAndVerify("k2")
    clientS1.get("k2") should ===(None)

    val secondary2 = Secondary.gen(arbiter, flakyPersistence, flakyReplication,
      s"${caseName}-secondary2")
    val clientS2 = session(secondary2)

    Thread.sleep(1000)

    clientS2.get("k1") should ===(Some("100"))
    clientS2.get("k2") should ===(None)

    clientP.setAcked("k2", "200")
    clientS1.get("k2") should ===(Some("200"))
    clientS2.get("k2") should ===(Some("200"))
    clientP.setAcked("k2", "201")
    clientS1.get("k2") should ===(Some("201"))
    clientS2.get("k2") should ===(Some("201"))

  }


}
