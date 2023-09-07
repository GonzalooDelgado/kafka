/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.time.Duration;

/** Used internally by MirrorMaker. Stores offset syncs and performs offset translation. */
class OffsetSyncStore implements AutoCloseable {
    private KafkaConsumer<byte[], byte[]> consumer;
    private Map<TopicPartition, OffsetSync> offsetSyncs = new HashMap<>();
    private TopicPartition offsetSyncTopicPartition;

    OffsetSyncStore(MirrorCheckpointConfig config) {
        Consumer<byte[], byte[]> consumer = null;
        TopicAdmin admin = null;
        KafkaBasedLog<byte[], byte[]> store;
        try {
            consumer = MirrorUtils.newConsumer(config.offsetSyncsTopicConsumerConfig());
            admin = new TopicAdmin(
                    config.offsetSyncsTopicAdminConfig(),
                    config.forwardingAdmin(config.offsetSyncsTopicAdminConfig()));
            store = createBackingStore(config, consumer, admin);
        } catch (Throwable t) {
            Utils.closeQuietly(consumer, "consumer for offset syncs");
            Utils.closeQuietly(admin, "admin client for offset syncs");
            throw t;
        }
        this.admin = admin;
        this.backingStore = store;
    }

    private KafkaBasedLog<byte[], byte[]> createBackingStore(MirrorCheckpointConfig config, Consumer<byte[], byte[]> consumer, TopicAdmin admin) {
        return new KafkaBasedLog<byte[], byte[]>(
                config.offsetSyncsTopic(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                () -> admin,
                (error, record) -> this.handleRecord(record),
                Time.SYSTEM,
                ignored -> {
                }
        ) {
            @Override
            protected Producer<byte[], byte[]> createProducer() {
                return null;
            }

            @Override
            protected Consumer<byte[], byte[]> createConsumer() {
                return consumer;
            }

            @Override
            protected boolean readPartition(TopicPartition topicPartition) {
                return topicPartition.partition() == 0;
            }
        };
    }

    OffsetSyncStore() {
        this.admin = null;
        this.backingStore = null;
    }

    /**
     * Start the OffsetSyncStore, blocking until all previous Offset Syncs have been read from backing storage.
     */
    public void start() {
        backingStore.start();
        readToEnd = true;
    }

    long translateDownstream(TopicPartition sourceTopicPartition, long upstreamOffset) {
        OffsetSync offsetSync = latestOffsetSync(sourceTopicPartition);
        if (offsetSync.upstreamOffset() > upstreamOffset) {
            // Offset is too far in the past to translate accurately
            return -1;
        }
        long upstreamStep = upstreamOffset - offsetSync.upstreamOffset();
        return offsetSync.downstreamOffset() + upstreamStep;
    }

    // poll and handle records
    synchronized void update(Duration pollTimeout) {
        try {
            consumer.poll(pollTimeout).forEach(this::handleRecord);
        } catch (WakeupException e) {
            // swallow
        }
    }

    public synchronized void close() {
        consumer.wakeup();
        Utils.closeQuietly(consumer, "offset sync store consumer");
    }

    protected void handleRecord(ConsumerRecord<byte[], byte[]> record) {
        OffsetSync offsetSync = OffsetSync.deserializeRecord(record);
        TopicPartition sourceTopicPartition = offsetSync.topicPartition();
        offsetSyncs.put(sourceTopicPartition, offsetSync);
    }

    private OffsetSync latestOffsetSync(TopicPartition topicPartition) {
        return offsetSyncs.computeIfAbsent(topicPartition, x -> new OffsetSync(topicPartition,
            -1, -1));
    }
}
