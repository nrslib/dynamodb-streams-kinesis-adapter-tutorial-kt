import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import kotlin.system.exitProcess

const val tablePrefix = "KCL-Demo"
const val srcTable = "$tablePrefix-src"
const val destTable = "$tablePrefix-dest"

fun main() {
    val awsCredentialProvider: DefaultAWSCredentialsProviderChain = DefaultAWSCredentialsProviderChain.getInstance()
    val awsRegion = Regions.US_EAST_1
    val dynamoDbClient = AmazonDynamoDBClientBuilder.standard()
        .withRegion(awsRegion)
        .build()

    val dynamoDbStreamsClient = AmazonDynamoDBStreamsClientBuilder.standard()
        .withRegion(awsRegion)
        .build()

    val streamArn = setUpTables(dynamoDbClient)
    val recordProcessorFactory = StreamsRecordProcessorFactory(dynamoDbClient, destTable)

    val workerConfig = KinesisClientLibConfiguration(
        "streams-adapter-demo",
        streamArn,
        awsCredentialProvider,
        "streams-demo-worker"
    )
        .withMaxRecords(1000)
        .withIdleMillisBetweenCalls(500)
        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

    val adapterClient = AmazonDynamoDBStreamsAdapterClient(dynamoDbStreamsClient)
    val cloudWatch = AmazonCloudWatchClientBuilder.standard()
        .withRegion(awsRegion)
        .build()

    val worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(
        recordProcessorFactory,
        workerConfig,
        adapterClient,
        dynamoDbClient,
        cloudWatch
    )

    println("Starting worker...")
    val t = Thread(worker)
    t.start()

    Thread.sleep(25000)
    worker.shutdown()
    t.join()

    val srcTableItems = StreamsAdapterDemoHelper.scanTable(dynamoDbClient, srcTable).items
    val destTableItems = StreamsAdapterDemoHelper.scanTable(dynamoDbClient, destTable).items
    if (srcTableItems.equals(destTableItems)) {
        println("Scan result is equal.")
    } else {
        println("Tables are different!")
    }

    println("Done.")
    cleanupAndExit(dynamoDbClient, 0);
}

fun setUpTables(dynamoDbClient: AmazonDynamoDB ): String {
    val streamArn = StreamsAdapterDemoHelper.createTable(dynamoDbClient, srcTable)
    StreamsAdapterDemoHelper.createTable(dynamoDbClient, destTable)

    awaitTableCreation(dynamoDbClient, srcTable)

    performOps(dynamoDbClient, srcTable)

    return streamArn
}

fun awaitTableCreation(dynamoDbClient: AmazonDynamoDB, tableName: String) {
    var retries = 0
    while(retries < 100) {
        val result = StreamsAdapterDemoHelper.describeTable(dynamoDbClient, tableName)
        val created = result.table.tableStatus == "ACTIVE"
        if (created) {
            println("Table is active.")
            return
        } else {
            retries++
            try {
                Thread.sleep(1000)
            }
            catch (e: InterruptedException) {
                // do nothing
            }
        }
    }
    println("Timeout after table creation. Exiting...")
    cleanupAndExit(dynamoDbClient, 1)
}

fun performOps(dynamoDbClient: AmazonDynamoDB, tableName: String) {
    StreamsAdapterDemoHelper.putItem(dynamoDbClient, tableName, "101", "test1")
    StreamsAdapterDemoHelper.updateItem(dynamoDbClient, tableName, "101", "test2")
    StreamsAdapterDemoHelper.deleteItem(dynamoDbClient, tableName, "101")
    StreamsAdapterDemoHelper.putItem(dynamoDbClient, tableName, "102", "demo3")
    StreamsAdapterDemoHelper.updateItem(dynamoDbClient, tableName, "102", "demo4")
    StreamsAdapterDemoHelper.deleteItem(dynamoDbClient, tableName, "102")
}

fun cleanupAndExit(dynamoDbClient: AmazonDynamoDB, returnValue: Int) {
    dynamoDbClient.deleteTable(DeleteTableRequest().withTableName(srcTable))
    dynamoDbClient.deleteTable(DeleteTableRequest().withTableName(destTable))
    exitProcess(returnValue)
}