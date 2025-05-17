package com.github.mobiusklein.mzd;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;


public class DictCodec {

    /**
     * Dictionary encode the provided data with indices
     */
    static <T extends Comparable<T>, V extends Comparable<V>> byte[] dictEncodeIndices(
            List<T> data, List<T> uniqueValues, Class<T> tClass, Class<V> vClass, Class<?> iClass) {

        int sizeOfV = Utils.getSizeOfType(vClass);
        int sizeOfI = Utils.getSizeOfType(iClass);

        int zData = sizeOfI * data.size();
        int zVals = uniqueValues.size();
        int zOffset = 8; // Size of long in bytes

        int totalSize = zOffset * 2 + (zVals * sizeOfV) + zData;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong((long) (zOffset * 2 + zVals * sizeOfV));
        buffer.putLong((long) zVals);

        // Add unique values to buffer
        for (T value : uniqueValues) {
            if (tClass == Byte.class || tClass == byte.class) {
                buffer.put((Byte)value);
            } else if (tClass == Short.class || tClass == short.class) {
                buffer.putShort((Short)value);
            } else if (tClass == Integer.class || tClass == int.class) {
                buffer.putInt(((Integer) value));
            } else if (tClass == Long.class || tClass == long.class) {
                buffer.putLong(((Long) value));
            } else if (tClass == Float.class || tClass == float.class) {
                buffer.putFloat(((Float) value));
            } else if (tClass == Double.class || tClass == double.class) {
                buffer.putDouble(((Double) value));
            }
        }

        // Create mapping of values to indices
        Map<T, Object> bytecodeMap = new HashMap<>();
        for (int i = 0; i < uniqueValues.size(); i++) {
            T val = uniqueValues.get(i);
            if (sizeOfI == 1) {
                bytecodeMap.put(val, (Object)(byte)i);
            } else if (sizeOfI == 2) {
                bytecodeMap.put(val, (Object)(short)i);
            } else if (sizeOfI == 4) {
                bytecodeMap.put(val, (Object) (int)i);
            } else if (sizeOfI == 8) {
                bytecodeMap.put(val, (Object) (long)i);
            }
        }

        // Add indices to buffer
        for (T value : data) {
            Object idxView = bytecodeMap.get(value);

            if (iClass == Byte.class || iClass == byte.class) {
                buffer.put((Byte)idxView);
            } else if (iClass == Short.class || iClass == short.class) {
                buffer.putShort((Short) idxView);
            } else if (iClass == Integer.class || iClass == int.class) {
                buffer.putInt((Integer) idxView);
            } else if (iClass == Long.class || iClass == long.class) {
                buffer.putLong((Long) idxView);
            }
        }

        return buffer.array();
    }

    /**
     * Dictionary encode with appropriate index type based on number of unique
     * values
     */
    static <T extends Comparable<T>, V extends Comparable<V>> byte[] dictEncodeValues(
            List<T> data, List<T> uniqueValues, Class<T> tClass, Class<V> vClass) {

        if (uniqueValues.size() < Math.pow(2, 8)) {
            return dictEncodeIndices(data, uniqueValues, tClass, vClass, Byte.class);
        } else if (uniqueValues.size() < Math.pow(2, 16)) {
            return dictEncodeIndices(data, uniqueValues, tClass, vClass, Short.class);
        } else if (uniqueValues.size() < Math.pow(2, 32)) {
            return dictEncodeIndices(data, uniqueValues, tClass, vClass, Integer.class);
        } else if (uniqueValues.size() < Math.pow(2, 64)) {
            return dictEncodeIndices(data, uniqueValues, tClass, vClass, Long.class);
        } else {
            throw new RuntimeException("Cannot encode a dictionary with more than 2 ** 64 values");
        }
    }

    /**
     * Dictionary encode the provided data
     */
    public static <T extends Comparable<T>> byte[] dictEncode(List<T> data, Class<T> tClass) {
        int sizeT = Utils.getSizeOfType(tClass);

        // Create sorted list of unique values
        Set<T> uniqueValuesSet = new HashSet<>(data);
        List<T> uniqValues = new ArrayList<>(uniqueValuesSet);
        Collections.sort(uniqValues);

        if (sizeT <= 1) {
            return dictEncodeValues(data, uniqValues, tClass, Byte.class);
        } else if (sizeT <= 2) {
            return dictEncodeValues(data, uniqValues, tClass, Short.class);
        } else if (sizeT <= 4) {
            return dictEncodeValues(data, uniqValues, tClass, Integer.class);
        } else if (sizeT <= 8) {
            return dictEncodeValues(data, uniqValues, tClass, Long.class);
        } else {
            throw new RuntimeException("Cannot encode a dictionary with more than 2 ** 64 unique keys");
        }
    }

    /**
     * Decode the values from the buffer
     */
    static <T> ArrayList<T> valueDecode(byte[] valueBuffer, int valueSize, Class<T> tClass) {
        ArrayList<T> uniqueValues = new ArrayList<>(valueBuffer.length / valueSize);
        ByteBuffer buffer = ByteBuffer.wrap(valueBuffer).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < valueBuffer.length; i += valueSize) {
            T val = Utils.bufferNext(buffer, tClass, valueSize);
            uniqueValues.add(val);
        }
        return uniqueValues;
    }

    /**
     * Decode the indices from the buffer
     */
    static <T, K> ArrayList<T> indexDecode(byte[] indexBuffer, List<T> uniqueValues, int numValues,
            Class<K> kClass) {
        ArrayList<T> values = new ArrayList<>(numValues);
        int indexWidth = Utils.getSizeOfType(kClass);
        ByteBuffer buffer = ByteBuffer.wrap(indexBuffer).order(ByteOrder.LITTLE_ENDIAN);

        for (int i = 0; i < indexBuffer.length; i += indexWidth) {
            int idx;
            if (indexWidth == 1) {
                idx = buffer.get() & 0xFF;
            } else if (indexWidth == 2) {
                idx = buffer.getShort() & 0xFFFF;
            } else if (indexWidth == 4) {
                idx = buffer.getInt();
            } else if (indexWidth == 8) {
                idx = (int) buffer.getLong();
            } else {
                throw new RuntimeException("Unsupported index size");
            }
            values.add(uniqueValues.get(idx));
        }

        return values;
    }

    /**
     * Dictionary decode the provided buffer
     */
    public static <T> ArrayList<T> dictDecode(byte[] buffer, Class<T> tClass) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
        long offset = byteBuffer.getLong();
        long numValues = byteBuffer.getLong();

        int valueSize = (int) ((offset - 16) / numValues);

        ArrayList<T> uniqueValues;
        byte[] valueBuffer = Arrays.copyOfRange(buffer, 16, (int) offset);
        byte[] indexBuffer = Arrays.copyOfRange(buffer, (int) offset, buffer.length);

        if (valueSize <= 1) {
            uniqueValues = valueDecode(valueBuffer, 1, tClass);
        } else if (valueSize <= 2) {
            uniqueValues = valueDecode(valueBuffer, 2, tClass);
        } else if (valueSize <= 4) {
            uniqueValues = valueDecode(valueBuffer, 4, tClass);
        } else if (valueSize <= 8) {
            uniqueValues = valueDecode(valueBuffer, 8, tClass);
        } else {
            throw new RuntimeException("Cannot decode values larger than 8 bytes a piece");
        }

        ArrayList<T> values;
        if (numValues <= Math.pow(2, 8)) {
            values = indexDecode(indexBuffer, uniqueValues, (int) numValues, Byte.class);
        } else if (numValues <= Math.pow(2, 16)) {
            values = indexDecode(indexBuffer, uniqueValues, (int) numValues, Short.class);
        } else if (numValues <= Math.pow(2, 32)) {
            values = indexDecode(indexBuffer, uniqueValues, (int) numValues, Integer.class);
        } else if (numValues <= Math.pow(2, 64)) {
            values = indexDecode(indexBuffer, uniqueValues, (int) numValues, Long.class);
        } else {
            throw new RuntimeException("Cannot decode more than 2 ** 64 values");
        }

        return values;
    }
}
