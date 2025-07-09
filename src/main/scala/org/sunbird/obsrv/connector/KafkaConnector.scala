package org.sunbird.obsrv.connector

import com.typesafe.config.Config
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetResetStrategy}
import org.json.{JSONException, JSONObject}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.connector.model.Models
import org.sunbird.obsrv.connector.source.{IConnectorSource, SourceConnector, SourceConnectorFunction}
import org.sunbird.obsrv.job.exception.UnsupportedDataFormatException
import org.sunbird.obsrv.job.model.Models.ErrorData

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.{Base64, Properties}

object KafkaConnector {

  def main(args: Array[String]): Unit = {
    SourceConnector.process(args, new KafkaConnectorSource)
  }
}

class KafkaConnectorSource extends IConnectorSource {

  private def base64ToTempFile(base64String: String, prefix: String): Path = {
    val cleanBase64 = base64String.replaceAll("\\s+", "") // Remove all whitespace
    val decoded = Base64.getDecoder.decode(cleanBase64)

    val tempFile = Files.createTempFile(prefix, ".jks")
    Files.write(tempFile, decoded)
    tempFile.toAbsolutePath
  }

  private def kafkaConsumerProperties(config: Config): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.getString("source_kafka_broker_servers"))
    properties.setProperty("group.id", config.getString("source_kafka_consumer_id"))
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    properties.setProperty("auto.offset.reset", config.getString("source_kafka_auto_offset_reset"))

    // Add SSL configuration if enabled
    if (config.hasPath("source_kafka_ssl_enabled") && config.getBoolean("source_kafka_ssl_enabled")) {
      // Create temporary files from base64 strings
      val truststorePath = base64ToTempFile(
        config.getString("source_kafka_ssl_truststore_base64"),
        "kafka-truststore"
      )
      val keystorePath = base64ToTempFile(
        config.getString("source_kafka_ssl_keystore_base64"),
        "kafka-keystore"
      )

      // Set SSL properties
      properties.setProperty("security.protocol", "SSL")
      properties.setProperty("ssl.truststore.location", truststorePath.toString)
      properties.setProperty("ssl.truststore.password", config.getString("source_kafka_ssl_truststore_password"))
      properties.setProperty("ssl.keystore.location", keystorePath.toString)
      properties.setProperty("ssl.keystore.password", config.getString("source_kafka_ssl_keystore_password"))
      properties.setProperty("ssl.key.password", config.getString("source_kafka_ssl_key_password"))

      // Add shutdown hook to cleanup temporary files
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        Files.deleteIfExists(truststorePath)
        Files.deleteIfExists(keystorePath)
      }))
    }

    properties
  }

  private def kafkaSource(config: Config): KafkaSource[String] = {
    val dataFormat = config.getString("source_data_format")
    if(!"json".equals(config.getString("source_data_format"))) {
      throw new UnsupportedDataFormatException(dataFormat)
    }
    KafkaSource.builder[String]()
      .setTopics(config.getString("source_kafka_topic"))
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(kafkaConsumerProperties(config))
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  override def getSourceStream(env: StreamExecutionEnvironment, config: Config): SingleOutputStreamOperator[String] = {
    env.fromSource(kafkaSource(config), WatermarkStrategy.noWatermarks[String](), config.getString("source_kafka_consumer_id")).uid(config.getString("source_kafka_consumer_id"))
  }

  override def getSourceFunction(contexts: List[Models.ConnectorContext], config: Config): SourceConnectorFunction = {
    new KafkaConnectorFunction(contexts)
  }
}

class KafkaConnectorFunction(contexts: List[Models.ConnectorContext]) extends SourceConnectorFunction(contexts) {

  override def getMetrics(): List[String] = List[String]()

  override def processEvent(event: String, onSuccess: String => Unit, onFailure: (String, ErrorData) => Unit, incMetric: (String, Long) => Unit): Unit = {

    if (event == null) {
      onFailure(event, ErrorData("EMPTY_JSON_EVENT", "Event data is null or empty"))
    } else if (!isValidJSON(event)) {
      onFailure(event, ErrorData("JSON_FORMAT_ERR", "Not a valid json"))
    } else {
      onSuccess(event)
    }
  }

  private def isValidJSON(json: String): Boolean = {
    try {
      new JSONObject(json)
      true
    }
    catch {
      case _: JSONException =>
        false
    }
  }

}

class StringDeserializationSchema extends KafkaRecordDeserializationSchema[String] {
  private val serialVersionUID = -3224825136576915426L
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
     try {
       out.collect(new String(record.value(), StandardCharsets.UTF_8))
     } catch {
       case ex: NullPointerException =>
         logger.error(s"Exception while parsing the message: ${ex.getMessage}")
     }
   }
}