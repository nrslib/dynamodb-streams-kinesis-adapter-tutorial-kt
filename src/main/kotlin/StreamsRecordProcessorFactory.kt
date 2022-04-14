import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory

class StreamsRecordProcessorFactory(private val dynamoDbClient: AmazonDynamoDB, private val tableName: String) : IRecordProcessorFactory {
    override fun createProcessor(): IRecordProcessor {
        return StreamsRecordProcessor(dynamoDbClient, tableName)
    }
}