package backtype.storm.utils;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;

/**
 * @author zhenchao.wang 2018-06-02 16:25
 * @version 1.0.0
 */
public class UtilsTest {

    @Test
    public void maybe_deserialize() throws Exception {
        byte[] bytes = FileUtils.readFileToByteArray(new File("../data/zk/assignments/passport-device-setting-topology-293-1527349170"));
        Object obj = Utils.maybe_deserialize(bytes);
        System.out.println(obj);
    }
}