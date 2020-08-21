package com.github.charithe.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KafkaJunitExtensionConfigTest {

    @Nested
    @KafkaJunitExtensionConfig
    static class DefaultConfigTest {

        @Test
        void default_values_are_set() {
            KafkaJunitExtensionConfig defaultConfig = DefaultConfigTest.class.getAnnotation(KafkaJunitExtensionConfig.class);
            Assertions.assertAll(() -> Assertions.assertEquals(KafkaJunitExtensionConfig.ALLOCATE_RANDOM_PORT, defaultConfig.kafkaPort()),
                                 () -> Assertions.assertEquals(KafkaJunitExtensionConfig.ALLOCATE_RANDOM_PORT, defaultConfig.zooKeeperPort()),
                                 () -> Assertions.assertEquals(StartupMode.DEFAULT, defaultConfig.startupMode()),
                                 () -> Assertions.assertEquals("", defaultConfig.propsPath()));
        }
    }

    @Nested
    @KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
    static class PartialConfigTest {

        @Test
        void unset_values_are_set_to_default() {
            KafkaJunitExtensionConfig defaultConfig = PartialConfigTest.class.getAnnotation(KafkaJunitExtensionConfig.class);
            Assertions.assertAll(() -> Assertions.assertEquals(KafkaJunitExtensionConfig.ALLOCATE_RANDOM_PORT, defaultConfig.kafkaPort()),
                                 () -> Assertions.assertEquals(KafkaJunitExtensionConfig.ALLOCATE_RANDOM_PORT, defaultConfig.zooKeeperPort()),
                                 () -> Assertions.assertEquals(StartupMode.WAIT_FOR_STARTUP, defaultConfig.startupMode()),
                                 () -> Assertions.assertEquals("", defaultConfig.propsPath()));
        }
    }

    @Nested
    @KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP, kafkaPort = 1234, zooKeeperPort = 5678, propsPath = "src/main/resources/config.properties")
    static class FullConfigTest {

        @Test
        void unset_values_are_set_to_default() {
            KafkaJunitExtensionConfig defaultConfig = FullConfigTest.class.getAnnotation(KafkaJunitExtensionConfig.class);
            Assertions.assertAll(() -> Assertions.assertEquals(1234, defaultConfig.kafkaPort()),
                                 () -> Assertions.assertEquals(5678, defaultConfig.zooKeeperPort()),
                                 () -> Assertions.assertEquals(StartupMode.WAIT_FOR_STARTUP, defaultConfig.startupMode()),
                                 () -> Assertions.assertEquals("src/main/resources/config.properties", defaultConfig.propsPath()));
        }
    }


}
