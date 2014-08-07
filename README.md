flume-couchbase-sink
====================

<!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

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