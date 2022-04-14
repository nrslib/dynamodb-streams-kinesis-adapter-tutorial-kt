import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import java.nio.charset.Charset

class StreamsRecordProcessor(private val dynamoDbClient: AmazonDynamoDB, private val tableName: String) : IRecordProcessor {
    private var checkpointCounter = 0

    override fun initialize(initializationInput: InitializationInput) {
        checkpointCounter = 0
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        processRecordsInput.records.forEach { record ->
            val data = String(record.data.array(), Charset.forName("UTF-8"))
            println(data)
            if (record is RecordAdapter) {
                val streamRecord = record.internalObject

                when (streamRecord.eventName) {
                    "INSERT", "MODIFY" ->
                        StreamsAdapterDemoHelper.putItem(dynamoDbClient, tableName, streamRecord.dynamodb.newImage)
                    "REMOVE" ->
                        StreamsAdapterDemoHelper.deleteItem(
                            dynamoDbClient,
                            tableName,
                            streamRecord.dynamodb.keys["Id"]!!.n
                        )
                }
                checkpointCounter += 1
                if (checkpointCounter % 10 == 0) {
                    try {
                        processRecordsInput.checkpointer.checkpoint()
                    } catch (e: Exception) {
                        e.printStackTrace()
                    }
                }
            }
        }
    }

    override fun shutdown(shutdownInput: ShutdownInput) {
        if (shutdownInput.shutdownReason == ShutdownReason.TERMINATE) {
            try {
                shutdownInput.checkpointer.checkpoint()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
}