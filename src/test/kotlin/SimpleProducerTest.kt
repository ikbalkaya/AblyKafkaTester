import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import kotlin.test.assertEquals

class SimpleProducerTest {
    @Test
    fun testProducerSentMessagesReceivedExactlyInTheSameOrder() = runBlocking {
        val messagePrefix = "Hi there x"
        val messageCount = 10
        val topicName = "topic1"
        val delay = 100L
        val job = launch {
            listenToMessages(listOf(topicName)).collectIndexed { index, value ->
                val message = value.data as ByteArray

                val messageString = message.toString(Charsets.UTF_8)
               // assertEquals(messageAtIndex(index, messagePrefix), messageString)
                if (index == messageCount - 1) {
                    println("All messages received")
                    cancel()
                }
            }
        }
        launch {
            delay(1000) //delay to allow subscriber to catch up
            produce(topic = topicName, messagePrefix = messagePrefix, messageCount = messageCount, delay = delay)
        }

        job.join()
    }

    //sent messages in 2 parallel channels and make sure they are independently received in their same order
    @Test
    fun testParallelSentMessagesSentToTwoDifferntTopicsReceivedInParallelOrder() = runBlocking {
        val messagePrefix = "First message"
        val messagePrefix2 = "Second message"
        val messageCount = 90
        val topicName = "topic1"
        val topicName2 = "topic2"
        val messageName = "${topicName}_message"
        val messageName2 = "${topicName2}_message"

        val delay = 50L
        val job = launch {
            var message1Index = 0
            var message2Index = 0
            listenToMessages(listOf(topicName,topicName2)).collectIndexed { index, value ->
                val message = value.data as ByteArray
                val messageString = message.toString(Charsets.UTF_8)
                if (value.name == messageName) {
                    assertEquals(messageAtIndex(message1Index, messagePrefix), messageString)
                    message1Index++
                } else if (value.name == messageName2) {
                    assertEquals(messageAtIndex(message2Index, messagePrefix2), messageString)
                    message2Index++
                }
                if (index == messageCount - 1) {
                    println("All messages received")
                    cancel()
                }
            }
        }
        launch {
            delay(1000) //delay to allow subscriber to catch up
            produce(topic = topicName,
                messagePrefix =  messagePrefix,
                messageCount = messageCount,
                delay = delay)
        }
        launch {
            delay(900) //delay to allow subscriber to catch up
            produce(topic = topicName2,
                messagePrefix =  messagePrefix2,
                messageCount = messageCount,
                delay = delay)
        }

        job.join()
    }


}