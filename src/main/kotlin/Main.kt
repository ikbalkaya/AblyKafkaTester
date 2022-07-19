import io.ably.lib.realtime.AblyRealtime
import io.ably.lib.realtime.CompletionListener
import io.ably.lib.types.ErrorInfo
import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*
import com.google.gson.Gson


fun main(args: Array<String>) = runBlocking {
    //trySendingMessages()
    val scope = CoroutineScope(Dispatchers.Default)
    scope.launch { listen("topic1") }
   // produce("topic1", "Message", "key1", 500)
    delay(200000)
}


fun listen(channelName:String) {
    val realtime = AblyRealtime("Lo4Cmg.BxYJqg:vnDrnPjyz6c0EDdyHeQbA--rv5xAf8KfDa_iv8hg194")
    realtime.channels.get(channelName).subscribe {
        println("Received message: ${Gson().toJson(it)}")
        println("Received message: ${it.data}")
       // val message = it.data as ByteArray
     //   val messageString = message.toString(Charsets.UTF_8)
       // println("Received message name: ${it.name} data: $messageString")
    }
}

private suspend fun produce(topic: String, key: String, messagePrefix: String, delay: Long) {

    val props = Properties()
    props.put("bootstrap.servers", "0.0.0.0:29092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = KafkaProducer<String, String>(props)
    try {
        for (i in 1..10) {
            delay(delay)
            val record = ProducerRecord(topic, key, "$messagePrefix $i")
            producer.send(record)
            println("Sent message: ${record.value()}")
        }
    } catch (e: Exception) {
        e.printStackTrace()
    } finally {
        producer.close()
    }
}
