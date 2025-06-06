/*
 * This source file was generated by the Gradle 'init' task
 */
package test.github.mobiusklein.mzd;

import org.junit.jupiter.api.Test;

import com.github.mobiusklein.mzd.*;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

class TestCodecs {

    @Test
    void testEncode() {
        Double[] data = new Double[5];
        data[0] = 1.0;
        data[1] = 2.0;
        data[2] = 3.0;
        data[3] = 4.0;
        data[4] = 5.0;
        byte[] buffer = MZDCodec.compress(data, double.class);
        ArrayList<Double> dup = MZDCodec.decompress(buffer, Double.class);
        for(int i = 0; i < dup.size(); i++) {
            Double val = dup.get(i);
            double ref = data[i];
            assert val == ref;
        }
    }

    @Test
    void testByteShuffleEncode() {
        Double[] data = new Double[5];
        data[0] = 1.0;
        data[1] = 2.0;
        data[2] = 3.0;
        data[3] = 4.0;
        data[4] = 5.0;
        byte[] buffer = MZDCodec.byteShuffleCompress(data, double.class);
        ArrayList<Double> dup = MZDCodec.byteShuffleDecompress(buffer, Double.class);
        for (int i = 0; i < dup.size(); i++) {
            Double val = dup.get(i);
            double ref = data[i];
            System.out.printf("%f vs %f\n", val, ref);
            assert val == ref;
        }
    }

    @Test
    void testDictEncode() {
        Double[] data = new Double[5];
        data[0] = 1.0;
        data[1] = 2.0;
        data[2] = 3.0;
        data[3] = 4.0;
        data[4] = 5.0;
        byte[] buffer = MZDCodec.dictionaryCompress(data, double.class);
        ArrayList<Double> dup = MZDCodec.dictionaryDecompress(buffer, Double.class);
        for (int i = 0; i < dup.size(); i++) {
            Double val = dup.get(i);
            double ref = data[i];
            System.out.printf("%f vs %f\n", val, ref);
            assert val == ref;
        }
    }
}
