/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.KinesisAsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.utils.FullJitterBackoff;
import org.apache.flink.util.Preconditions;

import io.netty.handler.timeout.ReadTimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** EFO subscription manager. */
@Internal
class FanOutKinesisShardSubscription {
    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSubscription.class);

    private final BlockingQueue<FanOutKinesisSubscriptionEvent> eventQueue = new LinkedBlockingQueue<>(2);
    private final AtomicReference<Throwable> subscriptionException = new AtomicReference<>();
    private final Duration queueWaitTimeout = Duration.ofSeconds(35);
    private final Duration subscriptionTimeout = Duration.ofSeconds(60);

    private Instant previousAttemptTimestamp;
    private volatile int attempts;
    private final int maxAttempts = 10;
    private final FullJitterBackoff backoff = new FullJitterBackoff();

    private final AtomicBoolean subscriptionActive = new AtomicBoolean(false);
    private final KinesisAsyncStreamProxy asyncStreamProxy;

    private final String consumerArn;
    private final String shardId;

    private StartingPosition startingPosition;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private FanOutShardSubscriber shardSubscriber;

    public FanOutKinesisShardSubscription(KinesisAsyncStreamProxy asyncStreamProxy, String consumerArn, String shardId, StartingPosition startingPosition) {
        Preconditions.checkNotNull(asyncStreamProxy);
        Preconditions.checkNotNull(consumerArn);
        Preconditions.checkNotNull(shardId);
        Preconditions.checkNotNull(startingPosition);

        this.asyncStreamProxy = asyncStreamProxy;
        this.consumerArn = consumerArn;
        this.shardId = shardId;
        this.startingPosition = startingPosition;

        this.previousAttemptTimestamp = Instant.now();
        this.attempts = 0;
    }

    public FanOutKinesisSubscriptionEvent nextEvent() {
        if ((shardSubscriber == null || !subscriptionActive.get()) && !finished.get()) {
            activateSubscription();
        }

        if (finished.get()) {
            if (shardSubscriber != null) {
                shardSubscriber = null;
            }
        }

        Throwable throwable = subscriptionException.get();
        if (throwable != null) {
            subscriptionException.set(null);
            shardSubscriber.cancel();
            if (attempts >= maxAttempts) {
                return FanOutKinesisSubscriptionEvent.fromThrowable(throwable);
            }
        }

        return eventQueue.poll();
    }

    public void close() {
        if (shardSubscriber != null) {
            shardSubscriber.cancel();
        }
    }

    private FanOutShardSubscriber getShardSubscriber() {
        return shardSubscriber;
    }

    private void activateSubscription() {
        if (finished.get()) {
            return;
        }
        if (subscriptionActive.get()) {
            LOG.warn("Skip subscription: already active");
            return;
        }
        if (startingPosition == null) {
            LOG.debug("Skip subscription: starting position is null");
            return;
        }

        long backoffDelay = backoff.calculateFullJitterBackoff(Duration.ofSeconds(2).toMillis(), Duration.ofSeconds(30).toMillis(), 2, attempts);

        if (previousAttemptTimestamp.plusMillis(backoffDelay).isAfter(Instant.now())) {
            return;
        }

        previousAttemptTimestamp = Instant.now();
        attempts++;

        subscriptionActive.set(true);

        CountDownLatch subscriptionLatch = new CountDownLatch(1);
        shardSubscriber = new FanOutShardSubscriber(subscriptionLatch);
        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler.builder()
                .subscriber(this::getShardSubscriber)
                .onError(exception -> {
                    if (subscriptionLatch.getCount() > 0) {
                        subscriptionException.set(new RuntimeException(exception));
                        subscriptionLatch.countDown();
                    }
                })
                .build();

        asyncStreamProxy.subscribeToShard(consumerArn, shardId, startingPosition, responseHandler);

        CompletableFuture.runAsync(() -> {
            try {
                boolean subscriptionTimedOut =
                        !subscriptionLatch.await(subscriptionTimeout.toMillis(), TimeUnit.MILLISECONDS);

                if (subscriptionTimedOut) {
                    LOG.warn("Subscription timed out");
                    shardSubscriber.cancel();
                } else if (subscriptionException.get() != null) {
                    shardSubscriber.cancel();
                } else {
                    // Request first record batch after successful subscription
                    shardSubscriber.requestRecords();
                    attempts = 0;
                }


            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                subscriptionException.set(new KinesisStreamsSourceException("", e));
            } catch (Exception e) {
                LOG.error("Unexpected exception thrown while subscribing to shard {} ({})", shardId, consumerArn, e);
                subscriptionException.set(new KinesisStreamsSourceException("", e));
            }
        });
    }

    /** Implementation of {@link Subscriber} to communicate with Kinesis EFO. */
    private class FanOutShardSubscriber implements Subscriber<SubscribeToShardEventStream> {
        private final CountDownLatch subscriptionLatch;
        private Subscription subscription;

        private volatile boolean cancelled;

        public FanOutShardSubscriber(CountDownLatch subscriptionLatch) {
            this.subscriptionLatch = subscriptionLatch;
        }

        public void requestRecords() {
            if (subscription != null) {
                subscription.request(1);
            }
        }

        public void cancel() {
            if (!subscriptionActive.get() && cancelled) {
                return;
            }

            subscriptionActive.set(false);
            cancelled = true;
            if (subscription != null) {
                subscription.cancel();
            }
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            this.subscriptionLatch.countDown();
            LOG.info("Successfully subscribed to shard {} at {} using consumer {}", shardId, startingPosition, consumerArn);
        }

        @Override
        public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
            subscribeToShardEventStream.accept(new SubscribeToShardResponseHandler.Visitor() {
                @Override
                public void visit(SubscribeToShardEvent event) {
                    enqueueEvent(event);
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            if (!isRecoverableError(throwable)) {
                if (!subscriptionException.compareAndSet(null, new RuntimeException(throwable))) {
                    LOG.warn("Another subscription exception had been queued, ignoring subsequent exceptions", throwable);
                }
                if (eventQueue.remainingCapacity() > 0) {
                    eventQueue.offer(FanOutKinesisSubscriptionEvent.fromThrowable(throwable));
                }
            } else {
                try {
                    cancel();
                } catch (Exception e) {
                    LOG.warn("ERROR WHILE TRYING TO CANCEL DIS", e);
                }
            }
        }

        @Override
        public void onComplete() {
            LOG.info("EFO subscription complete - {} ({})", shardId, consumerArn);
            subscriptionActive.set(false);
        }

        private boolean isRecoverableError(Throwable throwable) {
            return throwable instanceof ReadTimeoutException || throwable instanceof TimeoutException || throwable instanceof IOException;
        }

        private void enqueueEvent(SubscribeToShardEvent event) {
            if (cancelled) {
                return;
            }

            try {
                if (!eventQueue.offer(FanOutKinesisSubscriptionEvent.fromEvent(event), queueWaitTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out enqueuing event {} - shardId {} ({}). Subscription will be recreated.",
                            event.getClass().getSimpleName(), shardId, consumerArn);
                    cancel();
                } else {
                    startingPosition = StartingPosition.continueFromSequenceNumber(event.continuationSequenceNumber());
                    if (event.continuationSequenceNumber() == null) {
                        finished.set(true);
                    } else {
                        requestRecords();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KinesisStreamsSourceException("Read interrupted", e);
            }
        }
    }
}
