import io.ably.lib.realtime.AblyRealtime
import io.ably.lib.types.ChannelOptions
import io.ably.lib.types.Message
import io.ably.lib.util.Crypto
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.*


suspend fun produce(
    topic: String,
    key: String? = null,
    partition:Int = 0,
    messagePrefix: String,
    messageCount: Int,
    delay: Long,
    schemaContent:String? = null
) {

    val props = Properties()
    props.put("bootstrap.servers", "0.0.0.0:29092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", "http://0.0.0.0:8081");
    val producer = KafkaProducer<Any, Any>(props)
    try {
        for (i in 0..messageCount) {
            delay(delay)
            val message = messageAtIndex(i, messagePrefix)
            val record:ProducerRecord<Any,Any> = ProducerRecord(topic, partition, key, message)
         //  println("Producing $message")
            schemaContent?.let {
                val schematicRecord = createRecordWithSchema(it,key,topic,message, schemaUrl = "http://0.0.0.0:8081")
                producer.send(schematicRecord)
            }?: run {
                producer.send(record)
            }
        }
    } catch (e: Exception) {
        e.printStackTrace()
    } finally {
        producer.close()
    }
}

fun messageAtIndex(index: Int, messagePrefix: String): String {
    return "$messagePrefix $index"
}

fun listenToMessages(channelNames :List<String>): Flow<Message> {
    val realtime = AblyRealtime("Lo4Cmg.BxYJqg:vnDrnPjyz6c0EDdyHeQbA--rv5xAf8KfDa_iv8hg194")
    return callbackFlow {
        channelNames.forEach { channelName ->
            val channelOptions = ChannelOptions()
            channelOptions.encrypted = true
            channelOptions.cipherParams

            val base64Key = encryptionKey(channelName)
            val key = Base64.getDecoder().decode(base64Key)
            val params = Crypto.getDefaultParams(key)

            /* create a channel */

            /* create a channel */
            val channelOpts: ChannelOptions = object : ChannelOptions() {
                init {
                    encrypted = true
                    cipherParams = params
                }
            }

            realtime.channels.get(channelName/*,channelOpts*/).subscribe {
                val message = it.data as ByteArray
                println("Received message: ${it.name} ${message.toString(Charsets.UTF_8)}")
                trySend(it)
            }
        }
        awaitClose { cancel() }
    }
}

fun encryptMessage(key: String,message:String): String {
    val params = Crypto.getDefaultParams(encryptionKey(key))
    val cipherSet  = Crypto.createChannelCipherSet(params)
    val encrypted = cipherSet.encipher.encrypt(message.toByteArray(Charsets.UTF_8))
    return Base64.getEncoder().encodeToString(encrypted)
}

fun encryptionKey(channelName: String): String? {
    try {
        val digest = MessageDigest.getInstance("SHA-256")
        val hash = digest.digest(channelName.byteInputStream(StandardCharsets.UTF_8).readBytes())
        return Base64.getEncoder().encodeToString(hash)
    } catch (e: NoSuchAlgorithmException) {
        e.printStackTrace()
    }
    return null
}


fun createRecordWithSchema(schemaContent:String,key: String?,topic: String,message: String,schemaUrl:String): ProducerRecord<Any, Any> {
    val parser: Schema.Parser = Schema.Parser()
    val schema: Schema = parser.parse(schemaContent)
    registerSchema(schema,schemaUrl, subject = "$topic-value")
    val avroRecord: GenericRecord = GenericData.Record(schema)
    avroRecord.put("cardId", message)
    avroRecord.put("pocketId", message)
    avroRecord.put("cvv", message)
    avroRecord.put("limit", 1000)


    val record = ProducerRecord<Any, Any>(topic, key, avroRecord)
    return record
}

fun registerSchema(schema: Schema, schemaUrl: String, subject:String = "schemas") {

    val client = CachedSchemaRegistryClient(schemaUrl, 20)

    client.register(subject, schema)
}