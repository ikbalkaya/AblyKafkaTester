import io.ably.lib.realtime.AblyRealtime
import io.ably.lib.types.Message
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

suspend fun produce(
    topic: String,
    key: String? = null,
    partition:Int = 0,
    messagePrefix: String,
    messageCount: Int,
    delay: Long
) {

    val props = Properties()
    props.put("bootstrap.servers", "0.0.0.0:29092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = KafkaProducer<String, String>(props)
    try {
        for (i in 0..messageCount) {
            delay(delay)
            val message = messageAtIndex(i, messagePrefix)
            val record = ProducerRecord(topic, partition, key, message)
         //   val record = ProducerRecord(topic, key, message)
            producer.send(record)
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
            realtime.channels.get(channelName).subscribe {
                trySend(it)
            }
        }
        awaitClose { cancel() }
    }
}