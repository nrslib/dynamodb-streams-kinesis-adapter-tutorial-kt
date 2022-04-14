import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.*

object StreamsAdapterDemoHelper {
    fun createTable(client: AmazonDynamoDB, tableName: String): String {
        val attributeDefinition = listOf(
            AttributeDefinition()
                .withAttributeName("Id")
                .withAttributeType("N")
        )

        val keySchema = listOf(
            KeySchemaElement()
                .withAttributeName("Id")
                .withKeyType(KeyType.HASH)
        )

        val provisionedThroughput = ProvisionedThroughput()
            .withReadCapacityUnits(2L)
            .withWriteCapacityUnits(2L)

        val streamSpecification = StreamSpecification()
        streamSpecification.streamEnabled = true
        streamSpecification.setStreamViewType(StreamViewType.NEW_IMAGE)

        val createTableRequest = CreateTableRequest().withTableName(tableName)
            .withAttributeDefinitions(attributeDefinition)
            .withKeySchema(keySchema)
            .withProvisionedThroughput(provisionedThroughput)
            .withStreamSpecification(streamSpecification)

        return try {
            println("Create table $tableName")
            val result = client.createTable(createTableRequest)

            result.tableDescription.latestStreamArn
        } catch (e: ResourceInUseException) {
            println("Table already exists.")

            describeTable(client, tableName).table.latestStreamArn
        }
    }

    fun describeTable(client: AmazonDynamoDB, tableName: String): DescribeTableResult {
        return client.describeTable(DescribeTableRequest().withTableName(tableName))
    }

    fun scanTable(dynamoDbClient: AmazonDynamoDB, tableName: String): ScanResult {
        return dynamoDbClient.scan(ScanRequest().withTableName(tableName))
    }

    fun putItem(dynamoDbClient: AmazonDynamoDB, tableName: String, id: String, value: String) {
        val item = mapOf(
            "Id" to AttributeValue().withN(id),
            "attribute-1" to AttributeValue().withS(value)
        )

        val putRequest = PutItemRequest()
            .withTableName(tableName)
            .withItem(item)
        dynamoDbClient.putItem(putRequest)
    }

    fun putItem(dynamoDbClient: AmazonDynamoDB, tableName: String, items: Map<String, AttributeValue>) {
        val putItemRequest = PutItemRequest()
            .withTableName(tableName)
            .withItem(items)

        dynamoDbClient.putItem(putItemRequest)
    }

    fun updateItem(dynamoDbClient: AmazonDynamoDB, tableName: String, id: String, value: String) {
        val key = mapOf("Id" to AttributeValue().withN(id))

        val update = AttributeValueUpdate()
            .withAction(AttributeAction.PUT)
            .withValue(AttributeValue().withS(value))
        val attributeUpdates = mapOf("attribute-2" to update)

        val updateItemRequest = UpdateItemRequest()
            .withTableName(tableName)
            .withKey(key)
            .withAttributeUpdates(attributeUpdates)

        dynamoDbClient.updateItem(updateItemRequest)
    }

    fun deleteItem(dynamoDbClient: AmazonDynamoDB, tableName: String, id: String) {
        val key = mapOf("Id" to AttributeValue().withN(id))

        val deleteItemRequest = DeleteItemRequest()
            .withTableName(tableName)
            .withKey(key)

        dynamoDbClient.deleteItem(deleteItemRequest)
    }
}