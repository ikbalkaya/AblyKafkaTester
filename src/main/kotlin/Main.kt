import io.ably.lib.realtime.AblyRealtime
import io.ably.lib.realtime.CompletionListener
import io.ably.lib.types.ErrorInfo
import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

fun main(args: Array<String>) = runBlocking {
    //trySendingMessages()
    val scope = CoroutineScope(Dispatchers.Default)
    scope.launch { listen() }
    produce()
    delay(20000)
}

fun listen() {
    val realtime = AblyRealtime("Lo4Cmg.BxYJqg:vnDrnPjyz6c0EDdyHeQbA--rv5xAf8KfDa_iv8hg194")
    realtime.channels.get("topic1").subscribe {
        val message = it.data as ByteArray
        println("Received message: ${it.name} ${message.toString(Charsets.UTF_8)}")
    }
}

private suspend fun produce() {

    val props = Properties()
    props.put("bootstrap.servers", "0.0.0.0:29092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = KafkaProducer<String, String>(props)
    try {
        for (i in 1..10) {
            delay(1000)
            val record = ProducerRecord("topic1", "key", "Message $i")
            producer.send(record)
            println("Sent message: ${record.value()}")
        }
    } catch (e: Exception) {
        e.printStackTrace()
    } finally {
        producer.close()
    }
}

private fun trySendingMessages(){
    val realtime = AblyRealtime("Lo4Cmg.BxYJqg:vnDrnPjyz6c0EDdyHeQbA--rv5xAf8KfDa_iv8hg194")
    for (i in 1..10) {
        val message = "Message $i"
        realtime.channels.get("topic1").publish("topic1",message, object : CompletionListener {
            override fun onSuccess() {
                println("Sent message: $message")
            }

            override fun onError(p0: ErrorInfo?) {
                println("Error sending message: $message ${p0?.message}")
            }
        })
    }
}
