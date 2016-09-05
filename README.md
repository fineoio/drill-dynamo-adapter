# drill-dynamo-adapter

Drill Adapter for DynamoDB

## Requirements

 * maven3
 * java8

## Usage

Similar to any other Drill adapter. See ```DynamoStoragePluginConfig``` for the properties you can specify.

## Compound Key Mapper

Its a common pattern to put multiple logical key parts in the Hash or Range. Using a compound key mapper allows dynamo to parse
that key into its constituent parts on read.

See ```TestDynamoKeyMapper``` for an example

NOTE: This currently does not support mapping in the query side, so you have to generate the key queries knowning its a compound key.

## Testing

```
$ mvn clean install
```

Tests leverage [DynamoDBLocal](https://aws.amazon.com/blogs/aws/dynamodb-local-for-desktop-development/), so we have to copy dependencies to the ```target/``` directory at least once (and on every clean)
