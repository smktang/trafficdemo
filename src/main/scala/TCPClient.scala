import java.net.InetSocketAddress
import java.nio.ByteBuffer

import akka.actor._
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Main program
 */
object TCPClientApp extends App {
  val customConf = ConfigFactory.parseString("""
akka {
   loggers = ["akka.event.slf4j.Slf4jLogger"]
   log-dead-letters = 0
   loglevel = DEBUG
}
""")
  val host = if (args.size > 0) args(0) else "localhost"
  val port = if (args.size > 1) Integer.parseInt(args(1)) else 4200
  val numClients = if (args.size > 2) Integer.parseInt(args(2)) else 40000
  val clientManager: ActorRef = ActorSystem("test", ConfigFactory.load(customConf))
    .actorOf(Props(classOf[TCPClientManager], new InetSocketAddress(host, port), numClients))
  clientManager ! StartSession
}

/**
 * Settings
 */
object SessionConfig {
  val SESSION_DURATION = 30 minutes
  val PAUSE_DURATION = 30 seconds
}

/**
 * Clients manager actor.
 */
class TCPClientManager(remoteAddr: InetSocketAddress, numClients: Int) extends Actor with ActorLogging {
  import context.dispatcher

  val statistics = context.actorOf(Statistics.props(numClients))

  context.system.scheduler.schedule(0 milliseconds,
    3 seconds,
    statistics,
    WriteLog)

  val router = Router(RoundRobinRoutingLogic(), Vector.fill(numClients) {
    val r = context.actorOf(Props(classOf[TCPClient], remoteAddr, numClients, statistics))
    context watch r
    ActorRefRoutee(r)
  })

  def receive = {
    case StartSession =>
      log.info("Session starting")
      statistics ! Reset
      router.routees.foreach(_.send(InitConnection, self))
//      context.system.scheduler.scheduleOnce(SessionConfig.SESSION_DURATION, self, EndSession)

    case EndSession =>
      log.info("Session ending")
      router.routees.foreach(_.send(CloseConnection, self))
      context.system.scheduler.scheduleOnce(SessionConfig.PAUSE_DURATION, self, RestartSession)

    case RestartSession =>
      statistics ! WriteLog
      self ! StartSession
  }
}

/**
 * Client actor
 *
 */
class TCPClient(remoteAddr: InetSocketAddress, numClients: Int, statistics: ActorRef) extends Actor with ActorLogging {
  import context.system
  import context.dispatcher
  import scala.collection.{mutable => m}

  private var requestResponseBalance = 0
  private var counter : Long = 0
  private val latencyMap: m.HashMap[Long, Long] = new m.HashMap[Long, Long]()
  private val buffer: ByteBuffer = ByteBuffer.allocate(8)

  def processData(data : ByteString) : Unit = {
    def process(req : Long) = {
      val latency = java.lang.System.currentTimeMillis() - latencyMap.getOrElse(req, 0l)
      latencyMap.remove(req)
      statistics ! RemoveOutstandingRequest(1)
    }

    var toProcess = data
    if (buffer.position() > 0){
      //buffer has stuff, try to fill up to 8
      val firstReply = data.slice(0, 8-buffer.position())
      buffer.put(firstReply.toArray)
      if (buffer.position() == 8){
        //have full packet
        process(buffer.getLong(0))
        //set toProcess to be the slice to be processed
        toProcess = data.slice(firstReply.size, data.size)
        //clear the buffer
        buffer.clear()
      } else {
        return
      }
    }

    //bufferCount should be 0 if got here
    val groupedReplies = toProcess.grouped(8)
    groupedReplies.foreach(group => {
      if (group.size == 8){
        process(group.asByteBuffer.getLong)
      } else {
        //this should be the last loop, but size < 8, means partial packet
        //store
        group.copyToBuffer(buffer)
      }
    })
  }

  def receive = {

    case InitConnection =>
      IO(Tcp) ! Connect(remoteAddr)
      log.debug("Connection initialized")

    case CommandFailed(_: Connect) =>
      statistics ! RegisterConnectionFailure(1)
      context.system.scheduler.scheduleOnce(3 seconds, self, InitConnection)

    case c@Connected(remote, local) =>
      val connection = sender()
      connection ! Register(self)
      statistics ! RegisterConnection(1)

      requestResponseBalance = 0
      val tickScheduler = context.system.scheduler.schedule(0 seconds, 1 second, self, Tick)

      def cleanupClose() = {
        statistics ! RemoveOutstandingRequest(latencyMap.size)
        statistics ! RegisterWriteFailure(latencyMap.size)
        latencyMap.clear()
        statistics ! UnRegisterConnection(1)
        connection ! Close
        tickScheduler.cancel()
        context.unbecome()
        //reconnect
        self ! InitConnection
      }

      context become {
        case Tick =>
          connection ! Write(ByteString(java.nio.ByteBuffer.allocate(8).putLong(counter).array()))
          latencyMap.put(counter, java.lang.System.currentTimeMillis())
          counter += 1
          statistics ! AddOutstandingRequest(1)
        case CommandFailed(w: Write) =>
          log.error("Write Error: {}", w)
          statistics ! RegisterWriteFailure(1)
          cleanupClose()
        case Received(data) =>
          processData(data)
        case CloseConnection | PeerClosed =>
//          log.info("Closing...Missing replies: {}", latencyMap.size)
          cleanupClose()
//          statistics ! UnRegisterConnection
//          connection ! Close
//          tickScheduler.cancel()
//          context.unbecome()
//          //reconnect
//          self ! InitConnection
        case x : ErrorClosed =>
  //        log.info("Closing...Missing replies: {}", latencyMap.size)
          cleanupClose()
//          statistics ! UnRegisterConnection
//          connection ! Close
//          tickScheduler.cancel()
//          context.unbecome()
//          //reconnect
//          self ! InitConnection
        case x =>
          log.error("Unhandled: {}", x)
      }
  }
}

case object Tick

case object StartSession

case object RestartSession

case object EndSession

case object InitConnection

case object CloseConnection
