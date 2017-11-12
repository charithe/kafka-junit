/*
 * Copyright 2016 Charith Ellawala
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.charithe.kafka;

import com.google.common.util.concurrent.Futures;
import org.junit.rules.ExternalResource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

public class KafkaJunitRule extends ExternalResource {
    private final EphemeralKafkaBroker broker;
    private final StartupMode mode;

    public KafkaJunitRule(EphemeralKafkaBroker broker) {
        this(broker, StartupMode.DEFAULT);
    }

    public KafkaJunitRule(EphemeralKafkaBroker broker, StartupMode mode) {
        this.broker = requireNonNull(broker);
        this.mode = requireNonNull(mode);
    }

    @Override
    protected void before() throws Throwable {
        final CompletableFuture<Void> startFuture = broker.start();

        if (mode == StartupMode.WAIT_FOR_STARTUP) {
            Futures.getUnchecked(startFuture);
        }
    }

    @Override
    protected void after() {
        try {
            broker.stop();
        } catch (ExecutionException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Obtain a {@link KafkaHelper} that provides a bunch of convenience methods
     * @return KafkaHelper
     */
    public KafkaHelper helper() {
        return KafkaHelper.createFor(broker);
    }

    /**
     * @return A new instance of {@link KafkaJunitRule} that will wait for the broker to finish starting before
     *         executing tests.
     */
    public KafkaJunitRule waitForStartup() {
        return new KafkaJunitRule(broker, StartupMode.WAIT_FOR_STARTUP);
    }

    /**
     * @return A new instance of {@link KafkaJunitRule} that will NOT wait for the broker to finish starting before
     *         executing tests
     */
    public KafkaJunitRule dontWaitForStartup() {
        return new KafkaJunitRule(broker, StartupMode.DEFAULT);
    }

    public static KafkaJunitRule create() {
        return new KafkaJunitRule(EphemeralKafkaBroker.create());
    }

    private enum StartupMode {
        WAIT_FOR_STARTUP, DEFAULT
    }
}
