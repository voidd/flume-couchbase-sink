flume-couchbase-sink
====================

The flume-couchbase-sink allows you to use Couchbase as a Flume sink.

Getting Started
1. Clone it
2. mvn -DskipTests=true clean install 
3. Change configuration in your's Flume base


Sample configuration:
agent.sinks = loggerSink
agent.sinks.loggerSink.type = org.apache.flume.sink.couchbase.CouchBaseSink
agent.sinks.loggerSink.hostNames = http://192.168.128.1:8091/pools
agent.sinks.loggerSink.bucketName = default
agent.sinks.loggerSink.batchSize = 200
agent.sinks.loggerSink.ttl = 2505600
agent.sinks.loggerSink.bufferSize = 32768
agent.sinks.loggerSink.keyPrefix = dctm_log_