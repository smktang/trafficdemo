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
      val logMessage = s"Session Log: |Active Conn: ${connections}| Failed Conn: ${connectionFailures} |Write failures: ${writeFailures} |Total req: ${totalRequests} |OReq: $outstandingRequests}"
      log.info(logMessage)

    case Reset =>
      // reset
      connections = 0
      connectionFailures = 0
      writeFailures = 0
      startTime = System.currentTimeMillis()
      lostResponses = 0

    case AddOutstandingRequest =>
      outstandingRequests += 1
      totalRequests += 1

    case RemoveOutstandingRequest =>
      outstandingRequests -= 1

    case RegisterConnection =>
      connections += 1
    case UnRegisterConnection =>
      connections -= 1

    case RegisterConnectionFailure =>
      connectionFailures += 1

    case ReportLostResponses(numberOfLostResponses) =>
      lostResponses = lostResponses + numberOfLostResponses

    case RegisterWriteFailure =>
      writeFailures = writeFailures + 1
  }
}

case object AddOutstandingRequest

case object RemoveOutstandingRequest

case object RegisterConnection

case object UnRegisterConnection

case object RegisterConnectionFailure

case object RegisterWriteFailure

case class ReportLostResponses(numberOfLostResponses: Int)

case object WriteLog

case object Reset
