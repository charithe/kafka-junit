package com.github.charithe.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface KafkaJunitExtensionConfig {

    int ALLOCATE_RANDOM_PORT = -1;

    int kafkaPort() default ALLOCATE_RANDOM_PORT;

    int zooKeeperPort() default ALLOCATE_RANDOM_PORT;

    StartupMode startupMode() default StartupMode.DEFAULT;
    
    String propsFileName() default "";
    
}