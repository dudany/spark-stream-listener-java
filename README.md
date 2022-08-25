# spark stream listener

## Overview
When using kafka in spark stream it's hard to track the progress of the streaming application. The problem is caused because spark streaming is not using the consumer groups and not committing the offsets. This solution is showing how we can utilise the consumer groups committing to track the consumer group lag.

## Technicalities

We extended the StreamingQueryListener with DummySparkStreamListener, this class is called on each streaming batch and produces metrics events, you can read more about it here https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-StreamingQueryListener.html.

We create the spark stream application with consumer group prefix `.option("groupIdPrefix","test")` and use it as the consumer group for lag measuring.

When `onQueryProgress()` is called it means that the batch is finished successfully, then we get the topic partitions endoffsets and commit them to the "test" consumer group using KafkaConsumer.
