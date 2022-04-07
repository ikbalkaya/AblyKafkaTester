import io.ably.lib.realtime.AblyRealtime
import io.ably.lib.types.Message
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collectIndexed
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

class SimpleProducerTest {
    @Test
    fun testSimpleProducer() = runBlocking {
        val messagePrefix = "Hi there"
        val messageCount = 9
      //  val scope = CoroutineScope(Dispatchers.Default)
        val job = launch {
            listen("topic1").collectIndexed { index, value ->
                val message = value.data as ByteArray

                val messageString = message.toString(Charsets.UTF_8)

                assertEquals(messageAtIndex(index, messagePrefix), messageString)
                if (index == messageCount - 1) {
                    println("All messages received")
                   cancel()
                }
            }
        }

        produce("topic1", "myKey", messagePrefix, messageCount, 500)
        job.join()
    }


    fun listen(channelName: String): Flow<Message> {
        val realtime = AblyRealtime("Lo4Cmg.BxYJqg:vnDrnPjyz6c0EDdyHeQbA--rv5xAf8KfDa_iv8hg194")
        return callbackFlow {
            realtime.channels.get(channelName).subscribe {
                trySend(it)
            }
            awaitClose { cancel() }
        }
    }

    fun messageAtIndex(index: Int, messagePrefix: String): String {
        return "$messagePrefix $index"
    }

    private suspend fun produce(
        topic: String,
        key: String,
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
                val record = ProducerRecord(topic, key, message)
                producer.send(record)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            producer.close()
        }
    }

}