import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestA {
    public static Function<InputStream, Supplier<JsonElement>> jsonReader = input -> {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(input);
        BufferedReader reader = new BufferedReader(new InputStreamReader(bufferedInputStream));
        return () -> {
            try {
                String line = reader.readLine();
                if (line==null)return null;
                return JsonParser.parseString(line);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        };
    };

    public static Function<JsonElement, byte[]> jsonWriter = js -> {
        return js.toString().getBytes();
    };

    private TestRunner testRunner;

    @Test
    public void test(){
        StandardProcessor sp = new StandardProcessor.Builder<>()
                .withReader(jsonReader)
                .withWriter(jsonWriter)
                .withTransformation(Transformation.of(js->js))
                .withDefaultRelationship(new Relationship.Builder().name("kek").build())
                .build();
        testRunner = TestRunners.newTestRunner(sp);
        testRunner.enqueue("{\"key\":\"val\"}");
        testRunner.run();
        List<MockFlowFile> mockFlowFileList = testRunner.getFlowFilesForRelationship("kek");
        Assert.assertEquals(1, mockFlowFileList.size());
        mockFlowFileList.forEach(mockFlowFile -> System.out.println(mockFlowFile.getContent()));
    }
}
