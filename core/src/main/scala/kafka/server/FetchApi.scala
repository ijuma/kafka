package kafka.server

import java.util

import kafka.message.ZStdCompressionCodec
import kafka.network.RequestChannel
import kafka.security.auth.{ClusterAction, Read, Resource, Topic}
import kafka.server.QuotaFactory.{QuotaManagers, UnboundedQuota}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.utils.Time

import scala.collection._
import scala.collection.JavaConverters._

class FetchApi(kafkaApi: KafkaApi, fetchManager: FetchManager, metadataCache: MetadataCache,
               quotas: QuotaManagers, replicaManager: ReplicaManager,
               brokerTopicStats: BrokerTopicStats, time: Time, brokerId: Int) extends Logging {

  this.logIdent = s"[FetchApi brokerId=$brokerId] "

  import kafkaApi._

  def handleRequest(request: RequestChannel.Request) {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val fetchContext = fetchManager.newContext(
      fetchRequest.metadata,
      fetchRequest.fetchData,
      fetchRequest.toForget,
      fetchRequest.isFromFollower)

    def errorResponse[T >: MemoryRecords <: BaseRecords](error: Errors): FetchResponse.PartitionData[T] = {
      new FetchResponse.PartitionData[T](error, FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
        FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
    }

    val erroneous = mutable.ArrayBuffer[(TopicPartition, FetchResponse.PartitionData[Records])]()
    val interesting = mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        fetchContext.foreachPartition { (topicPartition, data) =>
          if (!metadataCache.contains(topicPartition))
            erroneous += topicPartition -> errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            interesting += (topicPartition -> data)
        }
      } else {
        fetchContext.foreachPartition { (part, _) =>
          erroneous += part -> errorResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
        }
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      fetchContext.foreachPartition { (topicPartition, data) =>
        if (!authorize(request.session, Read, Resource(Topic, topicPartition.topic, LITERAL)))
          erroneous += topicPartition -> errorResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicPartition))
          erroneous += topicPartition -> errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += (topicPartition -> data)
      }
    }

    def maybeConvertFetchedData(tp: TopicPartition,
                                partitionData: FetchResponse.PartitionData[Records]): FetchResponse.PartitionData[BaseRecords] = {
      val logConfig = replicaManager.getLogConfig(tp)

      if (logConfig.forall(_.compressionType == ZStdCompressionCodec.name) && versionId < 10) {
        trace(s"Fetching messages is disabled for ZStandard compressed partition $tp. Sending unsupported version response to $clientId.")
        errorResponse(Errors.UNSUPPORTED_COMPRESSION_TYPE)
      } else {
        // Down-conversion of the fetched records is needed when the stored magic version is
        // greater than that supported by the client (as indicated by the fetch request version). If the
        // configured magic version for the topic is less than or equal to that supported by the version of the
        // fetch request, we skip the iteration through the records in order to check the magic version since we
        // know it must be supported. However, if the magic version is changed from a higher version back to a
        // lower version, this check will no longer be valid and we will fail to down-convert the messages
        // which were written in the new format prior to the version downgrade.
        val unconvertedRecords = partitionData.records
        val downConvertMagic =
          logConfig.map(_.messageFormatVersion.recordVersion.value).flatMap { magic =>
            if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1 && !unconvertedRecords.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V0))
              Some(RecordBatch.MAGIC_VALUE_V0)
            else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3 && !unconvertedRecords.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V1))
              Some(RecordBatch.MAGIC_VALUE_V1)
            else
              None
          }

        downConvertMagic match {
          case Some(magic) =>
            // For fetch requests from clients, check if down-conversion is disabled for the particular partition
            if (!fetchRequest.isFromFollower && !logConfig.forall(_.messageDownConversionEnable)) {
              trace(s"Conversion to message format ${downConvertMagic.get} is disabled for partition $tp. Sending unsupported version response to $clientId.")
              errorResponse(Errors.UNSUPPORTED_VERSION)
            } else {
              try {
                trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
                // Because down-conversion is extremely memory intensive, we want to try and delay the down-conversion as much
                // as possible. With KIP-283, we have the ability to lazily down-convert in a chunked manner. The lazy, chunked
                // down-conversion always guarantees that at least one batch of messages is down-converted and sent out to the
                // client.
                new FetchResponse.PartitionData[BaseRecords](partitionData.error, partitionData.highWatermark,
                  partitionData.lastStableOffset, partitionData.logStartOffset, partitionData.abortedTransactions,
                  new LazyDownConversionRecords(tp, unconvertedRecords, magic, fetchContext.getFetchOffset(tp).get, time))
              } catch {
                case e: UnsupportedCompressionTypeException =>
                  trace("Received unsupported compression type error during down-conversion", e)
                  errorResponse(Errors.UNSUPPORTED_COMPRESSION_TYPE)
              }
            }
          case None => new FetchResponse.PartitionData[BaseRecords](partitionData.error, partitionData.highWatermark,
            partitionData.lastStableOffset, partitionData.logStartOffset, partitionData.abortedTransactions,
            unconvertedRecords)
        }
      }
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]]
      responsePartitionData.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        partitions.put(tp, new FetchResponse.PartitionData(data.error, data.highWatermark, lastStableOffset,
          data.logStartOffset, abortedTransactions, data.records))
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      // When this callback is triggered, the remote API call has completed.
      // Record time before any byte-rate throttling.
      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      var unconvertedFetchResponse: FetchResponse[Records] = null

      def createResponse(throttleTimeMs: Int): FetchResponse[BaseRecords] = {
        // Down-convert messages for each partition if required
        val convertedData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[BaseRecords]]
        unconvertedFetchResponse.responseData().asScala.foreach { case (tp, unconvertedPartitionData) =>
          if (unconvertedPartitionData.error != Errors.NONE)
            debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
              s"on partition $tp failed due to ${unconvertedPartitionData.error.exceptionName}")
          convertedData.put(tp, maybeConvertFetchedData(tp, unconvertedPartitionData))
        }

        // Prepare fetch response from converted data
        val response = new FetchResponse(unconvertedFetchResponse.error(), convertedData, throttleTimeMs,
          unconvertedFetchResponse.sessionId())
        response.responseData.asScala.foreach { case (topicPartition, data) =>
          // record the bytes out metrics only when the response is being sent
          brokerTopicStats.updateBytesOut(topicPartition.topic, fetchRequest.isFromFollower, data.records.sizeInBytes)
        }
        response
      }

      def updateConversionStats(send: Send): Unit = {
        send match {
          case send: MultiRecordsSend if send.recordConversionStats != null =>
            send.recordConversionStats.asScala.toMap.foreach {
              case (tp, stats) => updateRecordConversionStats(request, tp, stats)
            }
          case _ =>
        }
      }

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
        val responseSize = sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        trace(s"Sending Fetch response with partitions.size=${unconvertedFetchResponse.responseData().size()}, " +
          s"metadata=${unconvertedFetchResponse.sessionId()}")
        sendResponseExemptThrottle(request, createResponse(0), Some(updateConversionStats))
      } else {
        // Fetch size used to determine throttle time is calculated before any down conversions.
        // This may be slightly different from the actual response size. But since down conversions
        // result in data being loaded into memory, we should do this only when we are not going to throttle.
        //
        // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the
        // quotas have been violated. If both quotas have been violated, use the max throttle time between the two
        // quotas. When throttled, we unrecord the recorded bandwidth quota value
        val responseSize = fetchContext.getResponseSize(partitions, versionId)
        val timeMs = time.milliseconds()
        val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request)
        val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, responseSize, timeMs)

        val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
        if (maxThrottleTimeMs > 0) {
          // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
          // from the fetch quota because we are going to return an empty response.
          quotas.fetch.unrecordQuotaSensor(request, responseSize, timeMs)
          if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
            quotas.fetch.throttle(request, bandwidthThrottleTimeMs, sendResponse)
          } else {
            quotas.request.throttle(request, requestThrottleTimeMs, sendResponse)
          }
          // If throttling is required, return an empty response.
          unconvertedFetchResponse = fetchContext.getThrottledResponse(maxThrottleTimeMs)
        } else {
          // Get the actual response. This will update the fetch context.
          unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
          trace(s"Sending Fetch response with partitions.size=$responseSize, metadata=${unconvertedFetchResponse.sessionId}")
        }

        // Send the response immediately.
        sendResponse(request, Some(createResponse(maxThrottleTimeMs)), Some(updateConversionStats))
      }
    }

    if (interesting.isEmpty)
      processResponseCallback(Seq.empty)
    else {
      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchRequest.minBytes,
        fetchRequest.maxBytes,
        versionId <= 2,
        interesting,
        replicationQuota(fetchRequest),
        processResponseCallback,
        fetchRequest.isolationLevel)
    }
  }

  class SelectingIterator(val partitions: util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData[Records]],
                          val quota: ReplicationQuotaManager)
    extends util.Iterator[util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]]] {
    val iter = partitions.entrySet().iterator()

    var nextElement: util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]] = null

    override def hasNext: Boolean = {
      while ((nextElement == null) && iter.hasNext()) {
        val element = iter.next()
        if (quota.isThrottled(element.getKey)) {
          nextElement = element
        }
      }
      nextElement != null
    }

    override def next(): util.Map.Entry[TopicPartition, FetchResponse.PartitionData[Records]] = {
      if (!hasNext()) throw new NoSuchElementException()
      val element = nextElement
      nextElement = null
      element
    }

    override def remove() = throw new UnsupportedOperationException()
  }

  // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
  // traffic doesn't exceed quota.
  private def sizeOfThrottledPartitions(versionId: Short,
                                        unconvertedResponse: FetchResponse[Records],
                                        quota: ReplicationQuotaManager): Int = {
    val iter = new SelectingIterator(unconvertedResponse.responseData(), quota)
    FetchResponse.sizeOf(versionId, iter)
  }

  def replicationQuota(fetchRequest: FetchRequest): ReplicaQuota =
    if (fetchRequest.isFromFollower) quotas.leader else UnboundedQuota
}
