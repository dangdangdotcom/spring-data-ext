package com.dangdang.digital.spring.data.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StopWatch;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@SpringBootApplication
public class Application implements ApplicationRunner {

    private static final Logger LOGGER = LogManager.getLogger( Application.class );

    @Value( "${opsnum:1000}" )
    private int opsNum = 1000;

    @Value( "${threadsnum:64}" )
    private int threadsNum;

    @Value( "${fastmode:true}" )
    private boolean fastMode;

    @Value( "${randomdata:true}" )
    private boolean randomData;

    @Value( "${jsonify:false}" )
    private boolean jsonify;

    @Value( "${sleep:0}" )
    private long sleep;

    @Autowired
    private ApplicationContext ctx;

    @Bean
    JedisConnectionFactory jedisConnectionFactory() {
        JedisConnectionFactory jcf = new JedisConnectionFactory();
        jcf.setHostName( "10.4.37.22" );
        jcf.setPort( 6079 );
        jcf.setDatabase( 0 );
        jcf.setUsePool( true );
        jcf.setTimeout( 60000 );
        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal( 256 );
        cfg.setMaxIdle( 16 );
        jcf.setPoolConfig( cfg );
        return jcf;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplateNoCompress() {
        RedisSerializer<Object> ser = null;
        if ( jsonify ) {
            ser = new GenericToStringSerializer( Object.class );
        } else {
            ser = new JdkSerializationRedisSerializer();
        }
        final RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
        template.setConnectionFactory( jedisConnectionFactory() );
        template.setValueSerializer( ser );
        return template;
    }

    @Bean
    public Stat stat() {
        return new Stat();
    }

    public LZ4RedisSerializeDecorator lz4ser() {
        RedisSerializer<Object> ser = null;
        if ( jsonify ) {
            ser = new GenericToStringSerializer( Object.class );
        } else {
            ser = new JdkSerializationRedisSerializer();
        }
        Stat stat = stat();
        return new LZ4RedisSerializeDecorator( ser, true, 1024 ) {
            @Override
            protected void onSerialize( boolean compressed, int srcLen, int compressedLen ) {
                stat.onCompress( compressed, srcLen, compressedLen );
            }

            @Override
            protected void onDeserialize( boolean compressed, int srcLen, int compressedLen ) {
                stat.onDecompress( compressed, srcLen, compressedLen );
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

    public void run( ApplicationArguments args ) throws Exception {
        StringBuilder argsInfo = new StringBuilder( "\nProgram arguments: \n" );
        Iterator<String> it = args.getOptionNames().iterator();
        while ( it.hasNext() ) {
            String optname = it.next();
            argsInfo.append( optname );
            argsInfo.append( " = " );
            argsInfo.append( args.getOptionValues( optname ) );
            argsInfo.append( "\n" );
        }
        LOGGER.info( argsInfo );
        TimeUnit.SECONDS.sleep( 3 );

        Stat stat = ctx.getBean( Stat.class );

        ObjectMapper om = new ObjectMapper();
        List<Object> data;
        data = (List<Object>) om.readValue( new ClassPathResource( "data.json" ).getInputStream(), Object.class );
        if ( jsonify ) {
            for ( int i = 0; i < data.size(); i++ ) {
                String json = om.writeValueAsString( data.get( i ) );
                data.set( i, json );
            }
        }
        RedisTemplate<String, Object> rtNoCompress = (RedisTemplate<String, Object>) ctx.getBean( "redisTemplateNoCompress" );
        RedisTemplate<String, Object> rtCompress = (RedisTemplate<String, Object>) ctx.getBean( "redisTemplateCompress" );

        StopWatch watch = new StopWatch();
        watch.start();
        LOGGER.info( "TEST R/W JdkSerializationRedisSerializer..." );
        test( data, rtNoCompress, opsNum, "sde-test-jsrs-" );
        watch.stop();
        LOGGER.info( "Time elapsed: {}s", watch.getTotalTimeSeconds() );

        watch.start();
        LOGGER.info( "TEST R/W LZ4RedisSerializeDecorator(ST/{}) ...", getLz4Mode() );
        test( data, rtCompress, opsNum, "sde-test-lz4-" );
        watch.stop();
        LOGGER.info( "Time elapsed: {}s", watch.getTotalTimeSeconds() );
        stat.report();

        watch.start();
        LOGGER.info( "TEST R/W LZ4RedisSerializeDecorator(MT/{}), {} threads...", getLz4Mode(), threadsNum );
        CountDownLatch latch0 = new CountDownLatch( threadsNum );
        for ( int i = 0; i < threadsNum; i++ ) {
            final String threadName = String.valueOf( i );
            new Thread( () -> {
                test( data, rtCompress, opsNum, "sde-test-lz4-mt" + threadName + "-" );
                latch0.countDown();
            } ).start();
        }
        latch0.await();
        watch.stop();
        LOGGER.info( "Time elapsed: {}s", watch.getTotalTimeSeconds() );
        stat.report();

        watch.start();
        String keyForBigdata = "sde-test-lz4-mt-r";
        rtCompress.opsForValue().set( keyForBigdata, data.get( data.size() - 1 ) );
        CountDownLatch latch1 = new CountDownLatch( threadsNum );
        LOGGER.info( "TEST R LZ4RedisSerializeDecorator(MT/{}), {} threads...", getLz4Mode(), threadsNum );
        for ( int i = 0; i < threadsNum; i++ ) {
            new Thread( () -> {
                for ( int j = 0; j < opsNum; j++ ) {
                    try {
                        rtCompress.opsForValue().get( keyForBigdata );
                        if ( sleep > 0 ) TimeUnit.MILLISECONDS.sleep( sleep );
                    } catch ( Throwable t ) {
                        LOGGER.error( t.getMessage(), t );
                    }
                }
                latch1.countDown();
            } ).start();
        }
        latch1.await();
        watch.stop();
        LOGGER.info( "Time elapsed: {}s", watch.getTotalTimeSeconds() );
        stat.report();
    }

    private void test( List<Object> data, RedisTemplate<String, Object> template, int opsNum, String keyPrefix ) {

        ValueOperations<String, Object> ops = template.opsForValue();
        for ( int i = 0; i < opsNum; i++ ) {
            try {
                String key = keyPrefix + i;
                Object value = data.get( i % data.size() );
                ops.set( key, value );
                if ( !ObjectUtils.nullSafeEquals( value, ops.get( key ) ) ) {
                    throw new RuntimeException( "LZ4RedisSerializeDecorator defect" );
                }
                if ( sleep > 0 ) TimeUnit.MILLISECONDS.sleep( sleep );
            } catch ( Throwable t ) {
                LOGGER.error( t.getMessage(), t );
            }
        }
    }

    public String getLz4Mode() {
        return fastMode ? "FAST" : "HC";
    }

    private class Stat {

        private AtomicLong serTotalDecompressedLen = new AtomicLong( 0 );

        private AtomicLong serTotalCompressedLen = new AtomicLong( 0 );

        private AtomicLong serCompressedCount = new AtomicLong( 0 );

        private AtomicLong desCompressedCount = new AtomicLong( 0 );

        private AtomicLong serCount = new AtomicLong( 0 );

        private AtomicLong desCount = new AtomicLong( 0 );


        public synchronized void onCompress( boolean compressed, int srcLen, int compressedLen ) {
            serTotalDecompressedLen.getAndAdd( srcLen );
            serTotalCompressedLen.getAndAdd( compressedLen );
            if ( compressed ) serCompressedCount.getAndIncrement();
            serCount.getAndIncrement();
        }

        public synchronized void onDecompress( boolean compressed, int srcLen, int compressedLen ) {
            if ( compressed ) desCompressedCount.getAndIncrement();
            desCount.getAndIncrement();
        }

        public synchronized void report() {
            long cl = serTotalCompressedLen.get();
            long dl = serTotalDecompressedLen.get();
            LOGGER.info( "\n---  SERIALIZATION  ---\nCount: {}\nCompressed count: {}\nTotal compressed length: {}\nTotal decompressed length: {}\nTotal ratio: {}\n--- DESERIALIZATION ---\nCount: {}\nCompressed count: {}\n",
                serCount.get(), serCompressedCount.get(), cl, dl, Double.valueOf( dl ) / Double.valueOf( cl ),
                desCount.get(), desCompressedCount.get()
            );
            serTotalCompressedLen.set( 0 );
            serTotalDecompressedLen.set( 0 );
            serCompressedCount.set( 0 );
            serCount.set( 0 );

            desCompressedCount.set( 0 );
            desCount.set( 0 );
        }
    }

    public static void main( String... args ) throws Exception {
        SpringApplication.run( Application.class, args );
    }

}
