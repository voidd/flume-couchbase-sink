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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flume.sink.couchbase.CouchBaseSinkConstants.*;

public class CouchBaseSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory
            .getLogger(CouchBaseSink.class);
    private final CounterGroup counterGroup = new CounterGroup();
    List<URI> baseURIs = new ArrayList<URI>();
    private CouchbaseClient client = null;
    private SinkCounter sinkCounter;
    private int ttlMs = DEFAULT_TTL;
    private String bucketName = DEFAULT_BUCKET_NAME;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private String keyPrefix = DEFAULT_KEY_PREFIX;
    private String password;
    private CouchBaseEventSerializer eventSerializer;


    @Override
    public void configure(Context context) {
        if (StringUtils.isNotBlank(context.getString(HOSTNAMES))) {
            baseURIs = convertURI(StringUtils.deleteWhitespace(
                    context.getString(HOSTNAMES)).split(","));
        }
        Preconditions.checkState(baseURIs != null
                && baseURIs.size() > 0, "Missing Param:" + HOSTNAMES);

        if (StringUtils.isNotBlank(context.getString(BUCKET_NAME))) {
            this.bucketName = context.getString(BUCKET_NAME);
        }

        if (!bucketName.equals(DEFAULT_BUCKET_NAME) && StringUtils.isNotBlank(context.getString(PASSWORD))) {
            password = context.getString(PASSWORD);
        } else {
            password = "";
        }

        if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
            this.batchSize = Integer.getInteger(context.getString(BATCH_SIZE));
        }

        if (StringUtils.isNotBlank(context.getString(TTL))) {
            this.ttlMs = Integer.getInteger(context.getString(TTL));
            Preconditions.checkState(ttlMs >= 0, TTL
                    + " must be non-negative integer or 0!");
        }

        if (StringUtils.isNotBlank(context.getString(BUFFER_SIZE))) {
            this.bufferSize = Integer.getInteger(context.getString(BUFFER_SIZE));
        }


        if (StringUtils.isNotBlank(context.getString(KEY_PREFIX))) {
            this.keyPrefix = context.getString(KEY_PREFIX);
        }

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        String serializerClazz = DEFAULT_SERIALIZER_CLASS;
        if (StringUtils.isNotBlank(context.getString(SERIALIZER))) {
            serializerClazz = context.getString(SERIALIZER);
        }

        try {
            @SuppressWarnings("unchecked")
            Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class
                    .forName(serializerClazz);
            Configurable serializer = clazz.newInstance();

            if (serializer instanceof CouchBaseJsonSerializer) {
                eventSerializer = (CouchBaseJsonSerializer) serializer;
            } else {
                throw new IllegalArgumentException(serializerClazz
                        + " is not an CouchBaseEventSerializer");
            }
        } catch (Exception e) {
            logger.error("Could not instantiate event serializer.", e);
            Throwables.propagate(e);
        }
    }

    private List<URI> convertURI(String[] uri) {
        List<URI> baseURI = new ArrayList<URI>(uri.length);
        try {
            for (String u : uri) {
                baseURI.add(new URI(u));
            }
        } catch (URISyntaxException e) {
            logger.error("Exception was occurred during URI creation: ", e);
        }
        return baseURI;
    }

    @Override
    public void start() {
        logger.info("Starting {}...", getName());
        sinkCounter.start();
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            cfb.setReadBufferSize(bufferSize);
            client = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs, bucketName, password));
            sinkCounter.incrementConnectionCreatedCount();
        } catch (IOException e) {
            logger.error("Can't connect to CouchBase: ", e);
            sinkCounter.incrementConnectionFailedCount();
        }
        super.start();
        logger.info("Started {}.", getName());
    }

    @Override
    public void stop() {
        logger.info("CouchBase sink {} stopping");
        if (client != null) {
            client.shutdown();
        }
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("{} processing...", getName());
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();


        try {
            txn.begin();
            int count;
            for (count = 0; count < batchSize; ++count) {
                Event event = channel.take();

                if (event == null) {
                    status = Status.BACKOFF;
                    break;
                }

                client.set(keyPrefix + "_" + sinkCounter.getEventDrainAttemptCount(),
                        ttlMs, eventSerializer.getDocument(event));
            }
            if (count <= 0) {
                sinkCounter.incrementBatchEmptyCount();
                counterGroup.incrementAndGet("channel.underflow");
                status = Status.BACKOFF;
            } else {
                if (count < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }

                sinkCounter.addToEventDrainAttemptCount(count);
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(count);
            counterGroup.incrementAndGet("transaction.success");
        } catch (IOException e) {
            txn.rollback();
            counterGroup.incrementAndGet("transaction.rollback");
        } finally {
            txn.close();
        }
        return status;
    }

}