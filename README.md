# WHAT DOES THIS DO

This application publishes large numbers of records to DynamoDb asynchronously. It is intended
to be used as part of a Spark application that needs to write large quantities of data to DynamoDb
as well as do other things. In cases where the Spark application is performing CPU-intensive work
that may be blocked by making synchronous calls to Dynamo, this async record publisher shines.

## Canonical Example

As part of setup for a large gameday or manual testing run, a software engineer Jason needs to 
create 100 million test records. If each record is ~10kb, the total dataset will be ~1TB. However,
to ensure the system works as intended, each record needs to do its own setup in the system.
For example, in this dummy app, each `FooPojo` is expected to exist inside a Dynamo table already. 
To ensure these opaque-box tests succeed, each `FooPojo` thus must be also written to DynamoDb.

Jason has two goals:

1. Create 100MM `FooPojo`'s and dump them to an S3 folder they can be read from to initiate testing.
2. Ensure the indexes for the pojos from step #1 already exist in DynamoDb. This will be called the
**supplementary data**.

Being a smart engineer, Jason decides to use Spark-on-EMR to produce the large quantities of data he
needs. However, how can he do this as performantly as possible? Does he need two spark jobs, the
first to write the pojo's and the second to create the indexes? Is there a better way to do it?

# HOW MUCH LOAD CAN IT HANDLE?

It has written ~10tb of data to Dynamo (and 1tb downstream) in 30 minutes on an EMR cluster of
1k hosts. During local testing, it easily published 10mm records in 2minutes from a MacbookAir M1
2020 model with 8gb RAM. 

# HOW DOES IT WORK?

The `FooPojoSparkPublisherApp` demonstrates a basic example.

1) Create a fixed number of partitions. 
This number should be a function of how many executors are available for the job.
Partitions should be created at roughly the same time and
no new tasks should be scheduled on an executor that has already completed processing previous tasks.
*This is a bug, the solution for which is known but not yet implemented.
Message @denis-step privately if you encounter this issue*. 
2) Assign a uniform number of records to each partition. 
E.g. if creating 100MM records on 1k executors with 2 partitions per cluster, we would ideally
partition 100k records into each partition. 
3) For each partition, call `FooPojoCreator`. This is the CPU-intensive work. This step represents
creating high-fidelity test data, such as large JSON's with semantically meaningful values.
Compressed data or base64'ed data may be included. 
4) For all of the **supplementary data** required for each `FooPojo`, call the `DynamoDbPublisher`.
5) Await the `DynamoDbPublisher`.
6) Write out the `FooPojo`. All of the records should exist in DynamoDb.

## DynamoDbPublisher

This class is essentially a SingleReader/MultiWriter queue with a blocking call on the 
writer/producer when the queue grows too large. It relies on a background poller thread 
(the single reader) to drain records from the queue to Netty. It has two main tunable parameters:
`maxInFlight` (the max number of records Netty may handle concurrently) and the `maxQueueSize`
(the max number of records to backpressure before blocking).

In a nutshell, the features:

- Backpressures records
- Batches records
- Aggressively retries 


### Considerations


The simplest way to write the records asynchronously is to call the `DynamoAsyncClient` 
for each `FooPojo` directly and then await all of the returned `CompletableFuture`s. This would work
only for small number of records. Issues include (in order of importance):

- Netty will throw a `RuntimeException` if the number of pending items grows past the number of
max pending acquires. Clients must backpressure themselves. 
This is why we use a `LinkedBlockingQueue` in the `DynamoDbPublisher`. When the queue grows too
large, producers (the Spark partition worker thread) will block until there is space.

- Too many concurrent requests handled by Netty. It is possible to simply wrap the calls to Netty
with a semaphore that has the number of max leases equal to the number of max acquires for Netty.
This will hinder performance at a relatively low number of in-flight requests
(20k was the sweet spot on a `r4.xlarge` EC2 instance). Using the queue instead of just the 
single semaphore thus allows tuning both the in-flight requests *and* the number of buffered 
requests.

- Large number of `CompletableFuture` references. When partitions must publish large batches of
supplementary data, the number of futures with live references will grow linearly. This will create
memory issues unless handled directly. Futures are tricky to work with in Java because they hold
references to objects called within those futures, such as the large 10kb records discussed in the
canonical example. This would quickly exhaust the heap unless the programmer was careful in allowing
the records to be reclaimed while keeping the futures live.

# BUGS

This was a scrappy implementation for load testing so it is missing handling some edge cases. There
are both safety and progress failures known.


1) If one task on an executor finishes before another, and the queue is empty and there are
no in-flight requests, then any other partitions on the same executor will be suspect after
the publisher loop shuts down prematurely. They will either a) Not make progress and get blocked
indefinitely on publishing to the queue that is no longer being drained, or b) if the queue never
blocks, they will finish writing to the queue and report completion. In either case, no records
are published to Dynamo.
2) If a task starts on an executor that has already finished at least one task, the newly scheduled
task will not make progress. 

In practice, only #2 has been observed. This can be obviated by tightly controlling the number of 
executors and partitions. If all executors are available on job submission, and no partitions must
wait for an executor, this error mode is unlikely.
