package com.github.mobiusklein.mzd;

import java.nio.ByteBuffer;
import java.util.List;

public class Utils {
    /**
     * Get the size of the type in bytes
     */
    static int getSizeOfType(Class<?> clazz) {
        if (clazz == Byte.class) {
            return 1;
        } else if (clazz == Short.class) {
            return 2;
        } else if (clazz == Integer.class) {
            return 4;
        } else if (clazz == Long.class) {
            return 8;
        } else if (clazz == Float.class) {
            return 4;
        } else if (clazz == Double.class) {
            return 8;
        } else {
            return 8; // Default to 8 bytes
        }
    }

    static <T> void toBuffer(List<T> data, Class<T> tClass, ByteBuffer buffer) {
        for (T value : data) {
            if (value instanceof Byte) {
                buffer.put((Byte) value);
            } else if (value instanceof Short) {
                buffer.putShort((Short) value);
            } else if (value instanceof Integer) {
                buffer.putInt((Integer) value);
            } else if (value instanceof Long) {
                buffer.putLong((Long) value);
            } else if (value instanceof Float) {
                buffer.putFloat((Float) value);
            } else if (value instanceof Double) {
                buffer.putDouble((Double) value);
            } else {
                throw new RuntimeException("Unsupported value size");
            }
        }
    }

    static <T> T bufferNext(ByteBuffer buffer, Class<T> tClass, int valueSize) {
        T val;
        if (tClass == Double.class || tClass == double.class) {
            val = (T)Double.valueOf(buffer.getDouble());
        }
        else if (tClass == Float.class || tClass == float.class) {
            val = (T)Float.valueOf(buffer.getFloat());
        }
        else if (valueSize == 1) {
            val = tClass.cast(Byte.valueOf(buffer.get()));
        } else if (valueSize == 2) {
            val = tClass.cast(Short.valueOf(buffer.getShort()));
        } else if (valueSize == 4) {
            val = tClass.cast(Integer.valueOf(buffer.getInt()));
        } else if (valueSize == 8) {
            val = tClass.cast(Long.valueOf(buffer.getLong()));
        } else {
            throw new RuntimeException("Unsupported value size");
        }
        return val;
    }

}
