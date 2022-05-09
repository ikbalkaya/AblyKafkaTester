import io.ably.lib.realtime.AblyRealtime
import io.ably.lib.types.ChannelOptions
import io.ably.lib.types.Message
import io.ably.lib.util.Crypto
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.*
import javax.crypto.KeyGenerator


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
         //  println("Producing $message")
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

            realtime.channels.get(channelName,channelOpts).subscribe {
                val message = it.data as ByteArray
                println("Received message: ${it.name} ${message.toString(Charsets.UTF_8)}")
                trySend(it)
            }
        }
        awaitClose { cancel() }
    }
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