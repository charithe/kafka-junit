package com.github.charithe.kafka;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * <p>{@code KafkaJunitExtension} provides a kafka broker that is started and stopped for each test</p>
 * <p>
 * It is configured by the optional annotation @{@link KafkaJunitExtensionConfig} and provides
 * dependency injection for constructors and methods for the classes {@link KafkaHelper} and {@link EphemeralKafkaBroker}
 * </p>
 * <br>
 * <b>Usage:</b>
 * <pre>
 * {@code @ExtendWith(KafkaJunitExtension.class)
 *  @literal @KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
 *   class MyTestClass {
 *
 *      @literal @BeforeEach
 *       void setUp(KafkaHelper kafkaHelper, EphemeralKafkaBroker broker) {
 *
 *       }
 *  }
 * }
 * </pre>
 */
@KafkaJunitExtensionConfig
public class KafkaJunitExtension implements BeforeAllCallback, AfterEachCallback, BeforeEachCallback, ParameterResolver {

    private static final ExtensionContext.Namespace KAFKA_JUNIT = ExtensionContext.Namespace.create("kafka-junit");
    private static final KafkaJunitExtensionConfig DEFAULT_CONFIG = KafkaJunitExtension.class.getAnnotation(KafkaJunitExtensionConfig.class);

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        KafkaJunitExtensionConfig kafkaConfig = getKafkaConfig(extensionContext);
        EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(kafkaConfig.kafkaPort(), kafkaConfig.zooKeeperPort());
        extensionContext.getStore(KAFKA_JUNIT).put(EphemeralKafkaBroker.class, broker);
        extensionContext.getStore(KAFKA_JUNIT).put(StartupMode.class, kafkaConfig.startupMode());
        extensionContext.getStore(KAFKA_JUNIT).put(KafkaHelper.class, KafkaHelper.createFor(broker));
    }

    private static KafkaJunitExtensionConfig getKafkaConfig(ExtensionContext extensionContext) {
        return extensionContext.getElement().map(annotatedElement -> {
            if (annotatedElement.isAnnotationPresent(KafkaJunitExtensionConfig.class)) {
                return annotatedElement.getAnnotation(KafkaJunitExtensionConfig.class);
            } else {
                return DEFAULT_CONFIG;
            }
        }).orElse(DEFAULT_CONFIG);
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        CompletableFuture<Void> startFuture = getBroker(extensionContext).start();
        if (getStartupMode(extensionContext) == StartupMode.WAIT_FOR_STARTUP) {
            Futures.getUnchecked(startFuture);
        }
    }

    private static EphemeralKafkaBroker getBroker(ExtensionContext extensionContext) {
        return extensionContext.getStore(KAFKA_JUNIT).get(EphemeralKafkaBroker.class, EphemeralKafkaBroker.class);
    }

    private static StartupMode getStartupMode(ExtensionContext extensionContext) {
        return extensionContext.getStore(KAFKA_JUNIT).get(StartupMode.class, StartupMode.class);
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        try {
            getBroker(extensionContext).stop();
        } catch (ExecutionException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType().equals(KafkaHelper.class) || parameterContext.getParameter().getType().equals(EphemeralKafkaBroker.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        Class<?> parameterType = parameterContext.getParameter().getType();
        return extensionContext.getStore(KAFKA_JUNIT).get(parameterType, parameterType);
    }

}