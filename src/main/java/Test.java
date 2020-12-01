import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class Test {


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



	public static void main(String[] args) {
		StandardProcessor sp = new StandardProcessor.Builder<>()
				.withReader(jsonReader)
				.withTransformation(Transformation.of(JsonElement::toString))
				.withWriter(jsonWriter)
				.build();
	}

}
