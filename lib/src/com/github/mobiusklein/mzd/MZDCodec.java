package com.github.mobiusklein.mzd;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import com.github.luben.zstd.Zstd;

/**
 * MZDCodec class for compression operations
 */
public class MZDCodec {

    /**
     * Transpose bytes of data
     */
    private static <T> byte[] transposeBytes(List<T> data, Class<T> tClass) {
        int sizeT = Utils.getSizeOfType(tClass);
        int bytesOfData = sizeT * data.size();
        byte[] buffer = new byte[bytesOfData];

        int offset = 0;
        for (int i = 0; i < sizeT; i++) {
            for (int j = 0; j < data.size(); j++) {
                byte value;

                T num = data.get(j);
                ByteBuffer byteBuffer = ByteBuffer.allocate(sizeT).order(ByteOrder.LITTLE_ENDIAN);

                if (tClass == Byte.class || tClass == byte.class) {
                    byteBuffer.put(0, (byte) num);
                } else if (tClass == Short.class || tClass == short.class) {
                    byteBuffer.putShort(0, (short) num);
                } else if (tClass == Integer.class || tClass == int.class) {
                    byteBuffer.putInt(0, (int) num);
                } else if (tClass == Long.class || tClass == long.class) {
                    byteBuffer.putLong(0, (long) num);
                } else if (tClass == Float.class || tClass == float.class) {
                    byteBuffer.putFloat(0, (float) num);
                } else if (tClass == Double.class || tClass == double.class) {
                    byteBuffer.putDouble(0, (double) num);
                }
                value = byteBuffer.array()[i];
                buffer[offset++] = value;
            }
        }
        return buffer;
    }

    /**
     * Reverse transpose bytes of data
     */
    private static <T> ArrayList<T> reverseTransposeBytes(byte[] buffer, Class<T> tClass) {
        int sizeT = Utils.getSizeOfType(tClass);
        int itemsOfT = buffer.length / sizeT;

        ArrayList<T> data = new ArrayList<>(itemsOfT);
        for (int i = 0; i < itemsOfT; i++) {
            if (tClass == Byte.class) {
                data.add((T) Byte.valueOf((byte) 0));
            } else if (tClass == Short.class) {
                data.add((T) Short.valueOf((short) 0));
            } else if (tClass == Integer.class) {
                data.add((T) Integer.valueOf(0));
            } else if (tClass == Long.class) {
                data.add((T) Long.valueOf(0L));
            } else if (tClass == Float.class) {
                data.add((T) Float.valueOf(0.0f));
            } else if (tClass == Double.class) {
                data.add((T) Double.valueOf(0.0));
            } else {
                try {
                    data.add(tClass.getDeclaredConstructor().newInstance());
                } catch (Exception e) {
                    throw new RuntimeException("Cannot create instance of type: " + tClass.getName());
                }
            }
        }

        for (int i = 0; i < buffer.length; i++) {
            int itemIndex = i % itemsOfT;
            int byteIndex = i / itemsOfT;

            T item = data.get(itemIndex);
            byte value = buffer[i];

            // Create a new value with the updated byte
            ByteBuffer byteBuffer;
            if (tClass == Byte.class) {
                byteBuffer = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN);
                byteBuffer.put(0, value);
                data.set(itemIndex, (T) Byte.valueOf(byteBuffer.get(0)));
            } else if (tClass == Short.class) {
                byteBuffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
                byteBuffer.putShort(0, (short) item);
                byteBuffer.put(byteIndex, value);
                data.set(itemIndex, (T) Short.valueOf(byteBuffer.getShort(0)));
            } else if (tClass == Integer.class) {
                byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                byteBuffer.putInt(0, (int) item);
                byteBuffer.put(byteIndex, value);
                data.set(itemIndex, (T) Integer.valueOf(byteBuffer.getInt(0)));
            } else if (tClass == Long.class) {
                byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                byteBuffer.putLong(0, (long) item);
                byteBuffer.put(byteIndex, value);
                data.set(itemIndex, (T) Long.valueOf(byteBuffer.getLong(0)));
            } else if (tClass == Float.class) {
                byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                byteBuffer.putFloat(0, (float) item);
                byteBuffer.put(byteIndex, value);
                data.set(itemIndex, (T) Float.valueOf(byteBuffer.getFloat(0)));
            } else if (tClass == Double.class) {
                byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                byteBuffer.putDouble(0, (double) item);
                byteBuffer.put(byteIndex, value);
                data.set(itemIndex, (T) Double.valueOf(byteBuffer.getDouble()));
            }
        }

        return data;
    }

    /**
     * In-place delta encoding
     */
    private static <T extends Number> void inPlaceDeltaEncoding(T[] data) {
        if (data.length < 2) {
            return;
        }

        if (data[0] instanceof Integer) {
            Integer prev = (Integer) data[0];
            Integer offset = (Integer) data[0];
            for (int i = 1; i < data.length; i++) {
                Integer tmp = (Integer) data[i];
                data[i] = (T) Integer.valueOf(tmp + offset - prev);
                prev = tmp;
            }
        } else if (data[0] instanceof Long) {
            Long prev = (Long) data[0];
            Long offset = (Long) data[0];
            for (int i = 1; i < data.length; i++) {
                Long tmp = (Long) data[i];
                data[i] = (T) Long.valueOf(tmp + offset - prev);
                prev = tmp;
            }
        } else if (data[0] instanceof Float) {
            Float prev = (Float) data[0];
            Float offset = (Float) data[0];
            for (int i = 1; i < data.length; i++) {
                Float tmp = (Float) data[i];
                data[i] = (T) Float.valueOf(tmp + offset - prev);
                prev = tmp;
            }
        } else if (data[0] instanceof Double) {
            Double prev = (Double) data[0];
            Double offset = (Double) data[0];
            for (int i = 1; i < data.length; i++) {
                Double tmp = (Double) data[i];
                data[i] = (T) Double.valueOf(tmp + offset - prev);
                prev = tmp;
            }
        }
    }

    /**
     * In-place delta decoding
     */
    private static <T extends Number> void inPlaceDeltaDecoding(List<T> data) {
        if (data.size() < 2) {
            return;
        }

        if (data.get(0) instanceof Integer) {
            Integer offset = (Integer) data.get(0);
            Integer prev = (Integer) data.get(1);
            for (int i = 2; i < data.size(); i++) {
                Integer val = ((Integer) data.get(i)) + prev - offset;
                data.set(i, (T) val);
                prev = val;
            }
        } else if (data.get(0) instanceof Long) {
            Long offset = (Long) data.get(0);
            Long prev = (Long) data.get(1);
            for (int i = 2; i < data.size(); i++) {
                Long val = ((Long) data.get(i)) + prev - offset;
                data.set(i, (T) val);
                prev = val;
            }
        } else if (data.get(0) instanceof Float) {
            Float offset = (Float) data.get(0);
            Float prev = (Float) data.get(1);
            for (int i = 2; i < data.size(); i++) {
                Float val = ((Float) data.get(i)) + prev - offset;
                data.set(i, (T) val);
                prev = val;
            }
        } else if (data.get(0) instanceof Double) {
            Double offset = (Double) data.get(0);
            Double prev = (Double) data.get(1);
            for (int i = 2; i < data.size(); i++) {
                Double val = ((Double) data.get(i)) + prev - offset;
                data.set(i, (T) val);
                prev = val;
            }
        }
    }

    /**
     * Apply compression to a data buffer
     */
    public static <T> byte[] compress(List<T> data, Class<T> tClass) {
        byte[] byteData = new byte[data.size() * Utils.getSizeOfType(tClass)];
        ByteBuffer buffer = ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN);
        Utils.toBuffer(data, tClass, buffer);
        return Zstd.compress(byteData);
    }

    /**
     * Apply compression to a data buffer
     */
    public static <T> byte[] compress(T[] data, Class<T> tClass) {
        byte[] byteData = new byte[data.length * Utils.getSizeOfType(tClass)];
        ByteBuffer buffer = ByteBuffer.wrap(byteData).order(ByteOrder.LITTLE_ENDIAN);
        Utils.toBuffer(Arrays.asList(data), tClass, buffer);
        return Zstd.compress(byteData);
    }

    /**
     * Decompress a compressed buffer
     */
    public static <T> ArrayList<T> decompress(byte[] buffer, Class<T> tClass) {
        byte[] decompressed = Zstd.decompress(buffer, (int) Zstd.getFrameContentSize(buffer));

        ByteBuffer byteBuffer = ByteBuffer.wrap(decompressed).order(ByteOrder.LITTLE_ENDIAN);
        ArrayList<T> result = new ArrayList<>();

        int sizeT = Utils.getSizeOfType(tClass);
        int count = decompressed.length / sizeT;

        for (int i = 0; i < count; i++) {
            result.add(Utils.bufferNext(byteBuffer, tClass, sizeT));
        }

        return result;
    }

    /**
     * Apply byte shuffling and compression
     */
    public static <T> byte[] byteShuffleCompress(T[] data, Class<T> tClass) {
        byte[] buffer = transposeBytes(Arrays.asList(data), tClass);
        return Zstd.compress(buffer);
    }

    /**
     * Apply byte shuffling and compression
     */
    public static <T> byte[] byteShuffleCompress(List<T> data, Class<T> tClass) {
        byte[] buffer = transposeBytes(data, tClass);
        return Zstd.compress(buffer);
    }

    /**
     * Decompress and unshuffle bytes
     */
    public static <T> ArrayList<T> byteShuffleDecompress(byte[] buffer, Class<T> tClass) {
        byte[] decompressed = Zstd.decompress(buffer, (int) Zstd.getFrameContentSize(buffer));

        return reverseTransposeBytes(decompressed, tClass);
    }

    /**
     * Apply dictionary encoding and compression
     */
    public static <T extends Comparable<T>> byte[] dictionaryCompress(List<T> data, Class<T> tClass) {
        byte[] buffer = DictCodec.dictEncode(data, tClass);

        return Zstd.compress(buffer);
    }

    /**
     * Apply dictionary encoding and compression
     */
    public static <T extends Comparable<T>> byte[] dictionaryCompress(T[] data, Class<T> tClass) {
        byte[] buffer = DictCodec.dictEncode(Arrays.asList(data), tClass);

        return Zstd.compress(buffer);
    }

    /**
     * Decompress and decode dictionary
     */
    public static <T> ArrayList<T> dictionaryDecompress(byte[] buffer, Class<T> tClass) {
        byte[] decompressed = Zstd.decompress(buffer, (int) Zstd.getFrameContentSize(buffer));

        return DictCodec.dictDecode(decompressed, tClass);
    }

    // /**
    //  * Apply delta encoding, byte shuffling, and compression
    //  */
    // public static <T extends Number> byte[] deltaByteShuffleCompress(T[] data, Class<T> tClass) {
    //     inPlaceDeltaEncoding(data);
    //     return byteShuffleCompress(data, tClass);
    // }

    // /**
    //  * Decompress, unshuffle, and delta decode
    //  */
    // public static <T extends Number> List<T> deltaByteShuffleDecompress(byte[] buffer, Class<T> tClass) {
    //     List<T> data = byteShuffleDecompress(buffer, tClass);
    //     inPlaceDeltaDecoding(data);
    //     return data;
    // }
}
