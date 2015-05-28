import akka.actor.{Props, Actor, ActorLogging}

/**
 * @author Dmitri Carpov
 */
case object Statistics {
  def props(expectedNumberOfConnections: Int) = Props(new Statistics(expectedNumberOfConnections))
}

class Statistics(expectedNumberOfConnections: Int) extends Actor with ActorLogging {
  private var connections = 0
  private var connectionFailures = 0
  private var writeFailures = 0
  private var startTime = System.currentTimeMillis()
  private var lostResponses = 0
  private var outstandingRequests = 0
  private var totalRequests = 0

  override def receive: Receive = {
    case WriteLog =>
      val logMessage = s"Session Log: |Active Conn: ${connections}| Failed Conn: ${connectionFailures} |Write failures: ${writeFailures} |Total req: ${totalRequests} |OReq: $outstandingRequests"
      log.info(logMessage)

    case Reset =>
      // reset
      connections = 0
      connectionFailures = 0
      writeFailures = 0
      startTime = System.currentTimeMillis()
      lostResponses = 0

    case AddOutstandingRequest(count) =>
      outstandingRequests += count
      totalRequests += count

    case RemoveOutstandingRequest(count) =>
      outstandingRequests -= count

    case RegisterConnection(count) =>
      connections += count
    case UnRegisterConnection(count) =>
      connections -= count

    case RegisterConnectionFailure(count) =>
      connectionFailures += count

    case ReportLostResponses(count) =>
      lostResponses += count

    case RegisterWriteFailure(count) =>
      writeFailures += count
  }
}

case class AddOutstandingRequest(count : Int = 1)

case class RemoveOutstandingRequest(count : Int = 1)

case class RegisterConnection(count : Int = 1)

case class UnRegisterConnection(count : Int = 1)

case class RegisterConnectionFailure(count : Int = 1)

case class RegisterWriteFailure(count : Int = 1)

case class ReportLostResponses(count : Int = 1)

case object WriteLog

case object Reset
