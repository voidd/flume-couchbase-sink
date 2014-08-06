/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.couchbase;

public class CouchBaseSinkConstants {

    /**
     * Comma separated list of hostname:port, if the port is not present the
     * default port '9300' will be used</p>
     * Example:
     * <pre>
     *  127.0.0.1:92001,127.0.0.2:9300
     * </pre>
     */
    public static final String HOSTNAMES = "hostNames";

    /**
     * The name to index the document to, defaults to 'flume'</p>
     * The current date in the format 'yyyy-MM-dd' will be appended to this name,
     * for example 'foo' will result in a daily index of 'foo-yyyy-MM-dd'
     */
    public static final String BUCKET_NAME = "bucketName";

    /**
     * Maximum number of events the sink should take from the channel per
     * transaction, if available. Defaults to 100
     */
    public static final String BATCH_SIZE = "batchSize";

    /**
     * TTL in days, when set will cause the expired documents to be deleted
     * automatically, if not set documents will never be automatically deleted
     */
    public static final String TTL = "ttl";

    /**
     * TTL in days, when set will cause the expired documents to be deleted
     * automatically, if not set documents will never be automatically deleted
     */
    public static final String PASSWORD = "password";


    /**
     * TTL in days, when set will cause the expired documents to be deleted
     * automatically, if not set documents will never be automatically deleted
     */
    public static final String BUFFER_SIZE = "bufferSize";

    /**
     * TTL in days, when set will cause the expired documents to be deleted
     * automatically, if not set documents will never be automatically deleted
     */
    public static final String KEY_PREFIX = "keyPrefix";

    /**
     * The fully qualified class name of the serializer the sink should use.
     */
    public static final String SERIALIZER = "serializer";

    /**
     * DEFAULTS USED BY THE SINK
     */

    public static final int DEFAULT_TTL = 0;
    public static final int DEFAULT_BUFFER_SIZE = 32768;
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final String DEFAULT_BUCKET_NAME = "default";
    public static final String DEFAULT_KEY_PREFIX = "log_";
    public static final String DEFAULT_SERIALIZER_CLASS = "org.apache.flume." +
            "sink.couchbase.CouchBaseJsonSerializer";
}
