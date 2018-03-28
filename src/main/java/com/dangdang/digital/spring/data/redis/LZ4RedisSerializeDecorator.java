package com.dangdang.digital.spring.data.redis;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 基于LZ4算法的RedisSerializer装饰器
 *
 * @param <T> 串行化处理的目标类型
 */
public class LZ4RedisSerializeDecorator<T> implements RedisSerializer<T> {

    private final int compressThreshold;

    private final RedisSerializer<T> serializer;

    private LZ4Factory factory;

    private LZ4Compressor compressor;

    private LZ4FastDecompressor decompressor;

    private static final int COMPRESS_HEAD_SIZE = Integer.SIZE / Byte.SIZE;

    private static final int NOCOMPRESS_HEAD_SIZE = 1;

    /**
     * 构造函数
     *
     * @param serializer        被装饰的串行化器
     * @param fastMode          是否使用快速模式，快速模式压缩比低但是压缩的速度快
     *                          快速模式不会让解压缩的速度更快
     * @param compressThreshold 启用压缩的最小字节数
     */
    public LZ4RedisSerializeDecorator( RedisSerializer<T> serializer, boolean fastMode, int compressThreshold ) {
        this.serializer = serializer;
        this.compressThreshold = compressThreshold;
        factory = LZ4Factory.fastestInstance();
        if ( fastMode ) {
            compressor = factory.fastCompressor();
        } else {
            compressor = factory.highCompressor();
        }
        decompressor = factory.fastDecompressor();
    }

    public byte[] serialize( T src ) throws SerializationException {
        byte[] srcBytes = serializer.serialize( src );
        int srcLen = srcBytes.length;
        ByteBuffer destBuf;
        if ( srcLen < compressThreshold ) {
            destBuf = ByteBuffer.allocate( NOCOMPRESS_HEAD_SIZE + srcLen );
            destBuf.put( (byte) -1 );
            destBuf.put( srcBytes );
            onSerialize( false, srcLen, srcLen );
            return destBuf.array();
        } else {
            int maxDestLen = compressor.maxCompressedLength( srcLen );
            destBuf = ByteBuffer.allocate( COMPRESS_HEAD_SIZE + maxDestLen );
            destBuf.putInt( srcLen );
            byte[] destArray = destBuf.array();
            int compressedLength = compressor.compress( srcBytes, 0, srcLen, destArray, destBuf.position(), maxDestLen );
            onSerialize( true, srcLen, compressedLength );
            return Arrays.copyOfRange( destArray, 0, COMPRESS_HEAD_SIZE + compressedLength );
        }
    }

    public T deserialize( byte[] src ) throws SerializationException {
        if ( src == null ) return serializer.deserialize( src );
        ByteBuffer srcBuf = ByteBuffer.wrap( src );
        boolean noCompressed = srcBuf.array()[0] < 0;
        if ( noCompressed ) {
            byte[] destBytes = Arrays.copyOfRange( src, 1, src.length );
            return serializer.deserialize( destBytes );
        } else {
            int destLen = srcBuf.getInt();
            byte[] destBytes = new byte[destLen];
            decompressor.decompress( src, srcBuf.position(), destBytes, 0, destLen );
            return serializer.deserialize( destBytes );
        }
    }

    protected void onSerialize( boolean compressed, int srcLen, int compressedLen ) {
    }
}
