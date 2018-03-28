package com.dangdang.digital.spring.data.redis;

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
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StopWatch;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;


@SpringBootApplication
public class Application {

    private static final Logger LOGGER = LogManager.getLogger( Application.class );

    @Bean
    JedisConnectionFactory jedisConnectionFactory() {
        JedisConnectionFactory jcf = new JedisConnectionFactory();
        jcf.setHostName( "10.4.37.22" );
        jcf.setPort( 6079 );
        jcf.setDatabase( 0 );
        jcf.setUsePool( true );
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal( 256 );
        cfg.setMaxIdle( 16 );
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
    public Stat stat() {
        return new Stat();
    }

    public LZ4RedisSerializeDecorator lz4ser() {
        RedisSerializer<Object> ser = new GenericToStringSerializer<Object>( Object.class );
        Stat stat = stat();
        return new LZ4RedisSerializeDecorator( ser, true, 1024 ) {
            @Override
            protected void onSerialize( boolean compressed, int srcLen, int compressedLen ) {
                if ( compressed ) {
                    stat.on( srcLen, compressedLen );
                }
            }
        };
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplateCompress() {
        final RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
        template.setConnectionFactory( jedisConnectionFactory() );
        template.setValueSerializer( lz4ser() );
        return template;
    }

    public static void main( String[] args ) throws IOException, InterruptedException {
        ApplicationContext ctx = SpringApplication.run( Application.class, args );
        Stat stat = ctx.getBean( Stat.class );

        ObjectMapper om = new ObjectMapper();
        List<Object> data = (List<Object>) om.readValue( new ClassPathResource( "data.json" ).getInputStream(), Object.class );
        for ( int i = 0; i < data.size(); i++ ) {
            String json = om.writeValueAsString( data.get( i ) );
            data.set( i, json );
        }

        int opsNum = 1000;
        int threadsNum = Runtime.getRuntime().availableProcessors() * 8;

        RedisTemplate<String, Object> rtNoCompress = (RedisTemplate<String, Object>) ctx.getBean( "redisTemplateNoCompress" );
        RedisTemplate<String, Object> rtCompress = (RedisTemplate<String, Object>) ctx.getBean( "redisTemplateCompress" );

        LOGGER.info( "Test GenericToStringSerializer(ST)..." );
        test( data, rtNoCompress, opsNum, "gts-test-" );

        LOGGER.info( "Test LZ4RedisSerializeDecorator(ST,Fast)..." );
        test( data, rtCompress, opsNum, "lz4-test-" );
        stat.report();

        LOGGER.info( "Test LZ4RedisSerializeDecorator(MT,Fast), {} threads...", threadsNum );
        CountDownLatch latch = new CountDownLatch( threadsNum );
        for ( int i = 0; i < threadsNum; i++ ) {
            new Thread( () -> {
                test( data, rtCompress, opsNum, "lz4-test-" + Thread.currentThread().getId() + "-" );
                latch.countDown();
            } ).start();
        }
        latch.await();
        stat.report();
    }

    private static void test( List<Object> data, RedisTemplate<String, Object> template, int opsNum, String keyPrefix ) {
        StopWatch watch = new StopWatch();
        watch.start();
        ValueOperations<String, Object> ops = template.opsForValue();
        for ( int i = 0; i < opsNum; i++ ) {
            try {
                String key = keyPrefix + i;
                String value = (String) data.get( i % data.size() );
                ops.set( key, value );
                if ( !ObjectUtils.nullSafeEquals( value, ops.get( key ) ) ) {
                    throw new RuntimeException( "LZ4RedisSerializeDecorator defect" );
                }
            } catch ( Throwable t ) {
                LOGGER.error( t.getMessage(), t );
            }
        }
        watch.stop();
        LOGGER.info( "Time elapsed: {}s", watch.getTotalTimeSeconds() );
    }

    private static class Stat {

        private AtomicLong totalDecompressedLen = new AtomicLong( 0 );

        private AtomicLong totalCompressedLen = new AtomicLong( 0 );

        public synchronized void on( int srcLen, int compressedLen ) {
            totalDecompressedLen.getAndAdd( srcLen );
            totalCompressedLen.getAndAdd( compressedLen );
        }

        public synchronized void report() {
            long cl = totalCompressedLen.get();
            long dl = totalDecompressedLen.get();
            LOGGER.info( "\nTotal compressed length: {}\nTotal decompressed length:{}\nTotal ratio: {}",
                cl, dl, Double.valueOf( dl ) / Double.valueOf( cl )
            );
            totalCompressedLen.set( 0 );
            totalDecompressedLen.set( 0 );
        }
    }
}
