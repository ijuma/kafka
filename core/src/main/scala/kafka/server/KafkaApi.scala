package kafka.server

import kafka.controller.KafkaController
import kafka.network.RequestChannel
import kafka.security.auth._
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordConversionStats
import org.apache.kafka.common.requests.{AbstractControlRequest, AbstractRequest, AbstractResponse}

import scala.collection.JavaConverters._

class KafkaApi(requestChannel: RequestChannel, quotas: QuotaManagers,
               controller: KafkaController, brokerTopicStats: BrokerTopicStats,
               authorizer: Option[Authorizer], brokerId: Int) extends Logging {

  this.logIdent = s"[KafkaApi brokerId=$brokerId] "

  def authorize(session: RequestChannel.Session, operation: Operation, resource: Resource): Boolean =
    authorizer.forall(_.authorize(session, operation, resource))

  def handleError(request: RequestChannel.Request, e: Throwable) {
    val mayThrottle = e.isInstanceOf[ClusterAuthorizationException] || !request.header.apiKey.clusterAction
    error("Error when handling request: " +
      s"clientId=${request.header.clientId}, " +
      s"correlationId=${request.header.correlationId}, " +
      s"api=${request.header.apiKey}, " +
      s"body=${request.body[AbstractRequest]}", e)
    if (mayThrottle)
      sendErrorResponseMaybeThrottle(request, e)
    else
      sendErrorResponseExemptThrottle(request, e)
  }

  // Throttle the channel if the request quota is enabled but has been violated. Regardless of throttling, send the
  // response immediately.
  def sendResponseMaybeThrottle(request: RequestChannel.Request,
                                        createResponse: Int => AbstractResponse,
                                        onComplete: Option[Send => Unit] = None): Unit = {
    val throttleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request)
    quotas.request.throttle(request, throttleTimeMs, sendResponse)
    sendResponse(request, Some(createResponse(throttleTimeMs)), onComplete)
  }

  def sendErrorResponseMaybeThrottle(request: RequestChannel.Request, error: Throwable) {
    val throttleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request)
    quotas.request.throttle(request, throttleTimeMs, sendResponse)
    sendErrorOrCloseConnection(request, error, throttleTimeMs)
  }

  def sendResponseExemptThrottle(request: RequestChannel.Request,
                                         response: AbstractResponse,
                                         onComplete: Option[Send => Unit] = None): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, Some(response), onComplete)
  }

  def sendErrorResponseExemptThrottle(request: RequestChannel.Request, error: Throwable): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendErrorOrCloseConnection(request, error, 0)
  }

  def sendErrorOrCloseConnection(request: RequestChannel.Request, error: Throwable, throttleMs: Int): Unit = {
    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(throttleMs, error)
    if (response == null)
      closeConnection(request, requestBody.errorCounts(error))
    else
      sendResponse(request, Some(response), None)
  }

  def sendNoOpResponseExemptThrottle(request: RequestChannel.Request): Unit = {
    quotas.request.maybeRecordExempt(request)
    sendResponse(request, None, None)
  }

  def closeConnection(request: RequestChannel.Request, errorCounts: java.util.Map[Errors, Integer]): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    requestChannel.updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    requestChannel.sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  def sendResponse(request: RequestChannel.Request,
                           responseOpt: Option[AbstractResponse],
                           onComplete: Option[Send => Unit]): Unit = {
    // Update error metrics for each error code in the response including Errors.NONE
    responseOpt.foreach(response => requestChannel.updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala))

    val response = responseOpt match {
      case Some(response) =>
        val responseSend = request.context.buildResponse(response)
        val responseString =
          if (RequestChannel.isRequestLoggingEnabled) Some(response.toString(request.context.apiVersion))
          else None
        new RequestChannel.SendResponse(request, responseSend, responseString, onComplete)
      case None =>
        new RequestChannel.NoOpResponse(request)
    }
    sendResponse(response)
  }

  def sendResponse(response: RequestChannel.Response): Unit = {
    requestChannel.sendResponse(response)
  }

  def isBrokerEpochStale(brokerEpochInRequest: Long): Boolean = {
    // Broker epoch in LeaderAndIsr/UpdateMetadata/StopReplica request is unknown
    // if the controller hasn't been upgraded to use KIP-380
    if (brokerEpochInRequest == AbstractControlRequest.UNKNOWN_BROKER_EPOCH) false
    else {
      val curBrokerEpoch = controller.brokerEpoch
      if (brokerEpochInRequest < curBrokerEpoch) true
      else if (brokerEpochInRequest == curBrokerEpoch) false
      else throw new IllegalStateException(s"Epoch $brokerEpochInRequest larger than current broker epoch $curBrokerEpoch")
    }
  }

  def authorizeClusterAction(request: RequestChannel.Request): Unit = {
    if (!isAuthorizedClusterAction(request))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  def isAuthorizedClusterAction(request: RequestChannel.Request): Boolean = {
    authorize(request.session, ClusterAction, Resource.ClusterResource)
  }

  def authorizeClusterAlter(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, Alter, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  def authorizeClusterDescribe(request: RequestChannel.Request): Unit = {
    if (!authorize(request.session, Describe, Resource.ClusterResource))
      throw new ClusterAuthorizationException(s"Request $request is not authorized.")
  }

  def updateRecordConversionStats(request: RequestChannel.Request, tp: TopicPartition,
                                  conversionStats: RecordConversionStats): Unit = {
    val conversionCount = conversionStats.numRecordsConverted
    if (conversionCount > 0) {
      request.header.apiKey match {
        case ApiKeys.PRODUCE =>
          brokerTopicStats.topicStats(tp.topic).produceMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.produceMessageConversionsRate.mark(conversionCount)
        case ApiKeys.FETCH =>
          brokerTopicStats.topicStats(tp.topic).fetchMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.fetchMessageConversionsRate.mark(conversionCount)
        case _ =>
          throw new IllegalStateException("Message conversion info is recorded only for Produce/Fetch requests")
      }
      request.messageConversionsTimeNanos = conversionStats.conversionTimeNanos
    }
    request.temporaryMemoryBytes = conversionStats.temporaryMemoryBytes
  }

}
