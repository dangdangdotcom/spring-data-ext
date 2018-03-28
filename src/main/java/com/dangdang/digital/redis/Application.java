package com.dangdang.digital.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StopWatch;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;


@SpringBootApplication
public class Application {

    private static final LZ4Serializer LZ4_SER = new LZ4Serializer();

    private static final Logger LOGGER = LogManager.getLogger( Application.class );

    @Bean
    JedisConnectionFactory jedisConnectionFactory() {
        JedisConnectionFactory jcf = new JedisConnectionFactory();
        jcf.setHostName( "10.4.37.22" );
        jcf.setPort( 6079 );
        jcf.setDatabase( 0 );
        jcf.setUsePool( true );
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal( 100 );
        cfg.setMaxIdle( 10 );
        jcf.setPoolConfig( cfg );
        return jcf;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplateNoCompress() {
        final RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
        template.setConnectionFactory( jedisConnectionFactory() );
        template.setValueSerializer( new GenericToStringSerializer<Object>( Object.class ) );
        return template;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplateCompress() {
        final RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
        template.setConnectionFactory( jedisConnectionFactory() );
        template.setValueSerializer( LZ4_SER );
        return template;
    }

    public static void main( String[] args ) throws IOException, InterruptedException {
        ApplicationContext ctx = SpringApplication.run( Application.class, args );
        ObjectMapper om = new ObjectMapper();
        List<Object> data = (List<Object>) om.readValue( new ClassPathResource( "data.json" ).getInputStream(), Object.class );
        for ( int i = 0; i < data.size(); i++ ) {
            String json = om.writeValueAsString( data.get( i ) );
            data.set( i, json );
        }

        int opsNum = 1000;
        int threadsNum = Runtime.getRuntime().availableProcessors() * 4;

        RedisTemplate<String, Object> rtNoCompress = (RedisTemplate<String, Object>) ctx.getBean( "redisTemplateNoCompress" );
        RedisTemplate<String, Object> rtCompress = (RedisTemplate<String, Object>) ctx.getBean( "redisTemplateCompress" );

        LOGGER.info( "Test GenericToStringSerializer(ST)..." );
        test( data, rtNoCompress, opsNum );
        LOGGER.info( "Test LZ4Serializer(ST)..." );

        test( data, rtCompress, opsNum );
        LZ4_SER.stat();

        LOGGER.info( "Test LZ4Serializer(MT), {} threads...", threadsNum );
        CountDownLatch latch = new CountDownLatch( threadsNum );
        for ( int i = 0; i < threadsNum; i++ ) {
            new Thread( () -> {
                test( data, rtCompress, opsNum );
                latch.countDown();
            } ).start();
        }
        latch.await();
        LZ4_SER.stat();
    }

    private static void test( List<Object> data, RedisTemplate<String, Object> template, int opsNum ) {
        StopWatch watch = new StopWatch();
        watch.start();
        ValueOperations<String, Object> ops = template.opsForValue();
        for ( int i = 0; i < opsNum; i++ ) {
            try {
                String key = "lz4-test-" + i;
                String value = (String) data.get( i % data.size() );
                ops.set( key, value );
                if ( !ObjectUtils.nullSafeEquals( value, ops.get( key ) ) ) {
                    throw new RuntimeException( "LZ4Serializer defect" );
                }
            } catch ( Throwable t ) {
                LOGGER.error( t.getMessage(), t );
            }
        }
        watch.stop();
        LOGGER.info( "Time elapsed: {}s", watch.getTotalTimeSeconds() );
    }
}
