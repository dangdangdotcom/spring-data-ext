package com.dangdang.digital.redis;

import net.jpountz.lz4.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;


public class LZ4Serializer implements RedisSerializer<String> {

    private static final Logger LOGGER = LogManager.getLogger( LZ4Serializer.class );

    private LZ4Factory factory;

    private LZ4Compressor compressor;

    private LZ4FastDecompressor decompressor;

    // 头部4bytes正整数，保存未压缩时数据的长度
    // 如果不启用压缩，则头部设置为-1，仅仅占1byte（此Demo未实现）
    private static final int HEAD_SIZE = Integer.SIZE / Byte.SIZE;

    public LZ4Serializer() {
        factory = LZ4Factory.fastestInstance();
        compressor = factory.fastCompressor();
        decompressor = factory.fastDecompressor();
    }

    public byte[] serialize( String src ) throws SerializationException {
        byte[] srcData = src.getBytes();
        int srcLen = srcData.length;
        totalDecompressedLen.getAndAdd( srcLen );
        int maxDestLen = compressor.maxCompressedLength( srcLen );
        ByteBuffer destBuf = ByteBuffer.allocate( HEAD_SIZE + maxDestLen );
        destBuf.putInt( srcLen );
        byte[] destArray = destBuf.array();
        int compressedLength = compressor.compress( srcData, 0, srcLen, destArray, destBuf.position(), maxDestLen );
        totalCompressedLen.getAndAdd( compressedLength );
        byte[] destData = Arrays.copyOfRange( destArray, 0, HEAD_SIZE + compressedLength );
        LOGGER.debug( "Original length {}, compressed length {}, Ratio {}",
            () -> srcLen, () -> compressedLength,
            () -> Double.valueOf( srcLen ) / Double.valueOf( compressedLength )
        );
        return destData;
    }

    public String deserialize( byte[] src ) throws SerializationException {
        if ( src == null ) return null;
        ByteBuffer srcBuf = ByteBuffer.wrap( src );
        int destLen = srcBuf.getInt();
        byte[] destData = new byte[destLen];
        decompressor.decompress( src, srcBuf.position(), destData, 0, destLen );
        return new String( destData );
    }

    /********* For test purpose ********/
    private AtomicLong totalDecompressedLen = new AtomicLong( 0 );

    private AtomicLong totalCompressedLen = new AtomicLong( 0 );

    public void stat() {
        long cl = totalCompressedLen.get();
        long dl = totalDecompressedLen.get();
        LOGGER.info( "\nTotal compressed length: {}\nTotal decompressed length:{}\nTotal ratio: {}",
            cl, dl, Double.valueOf( dl ) / Double.valueOf( cl )
        );
        totalCompressedLen.set( 0 );
        totalDecompressedLen.set( 0 );
    }
    /***********************************/
}
