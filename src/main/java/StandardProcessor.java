import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StandardProcessor<T,M> extends AbstractProcessor {

	List<Transformation<T,M>> transformations = new ArrayList<>();
	Function<InputStream, Supplier<T>> reader;
	Function<M, byte[]> writer;

	public StandardProcessor(){}

	private StandardProcessor(Builder<T,M> builder){
		this.transformations = builder.transformations;
		this.reader = builder.reader;
		this.writer = builder.writer;
	}

	protected StandardProcessor(Function<InputStream, Supplier<T>> reader, Function<M, byte[]> writer){
		this.reader = reader;
		this.writer = writer;
	}

	public static <T,M> StandardProcessor<T,M> of(Function<InputStream, Supplier<T>> reader, Function<M, byte[]> writer){
		return new StandardProcessor<>();
	}

	public StandardProcessor withReader(Function<InputStream, Supplier<T>> reader){
		this.reader = reader;
		return this;
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		LinkedList<FlowFile> flowFiles = new LinkedList<>(session.get(10));
		List<Pipeline<T,M>> pipelines = transformations.stream()
				.collect(Collectors.groupingBy(Transformation::getRelationship))
				.values().stream()
				.map(transformations -> Pipeline.of(transformations, writer, session.create(), session, getLogger()))
				.collect(Collectors.toList());

		flowFiles.forEach(file -> {
			InputStream inputStream = session.read(file);
			Stream.generate(reader.apply(inputStream))
				.takeWhile(Objects::nonNull)
				.forEach(record -> pipelines.forEach(pipeline -> pipeline.execute(record, file)));
			try {
				inputStream.close();
			} catch (IOException e) {
				getLogger().error("errlllkor");
			}
		});
		pipelines.forEach(Pipeline::close);
		session.remove(flowFiles);
	}

/*	public StandardProcessor<T, M> withTransformation(Transformation<T,M> transformation){
		transformations.add(transformation);
		return this;
	}

	public StandardProcessor<T, M> withTransformations(List<Transformation<T,M>> transformations){
		this.transformations.addAll(transformations);
		return this;
	}*/

	public static class Builder<T,M>{

		List<Transformation<T,M>> transformations = new ArrayList<>();
		Function<InputStream, Supplier<T>> reader;
		Function<M, byte[]> writer;

		public Builder(){}


		public Builder(Builder<T,M> builder){
			this.reader = builder.reader;
			this.writer = builder.writer;
			this.transformations = builder.transformations;
		}

		public Builder(Function<InputStream, Supplier<T>> reader, Function<M, byte[]> writer){
			this.reader = reader;
			this.writer = writer;
		}

		public <K> Builder<T,K> withWriter(Function<K, byte[]> writer){
			return new Builder<T,K>(reader, writer);
		}

		public <K> Builder<K,M> withReader(Function<InputStream, Supplier<K>> reader){
			return new Builder<>(reader, writer);
		}

		public Builder<T,M> withTransformation(Transformation<T,M> transformation){
			transformations.add(transformation);
			return new Builder<>(this);
		}

		public StandardProcessor<T,M> build(){
			return new StandardProcessor<>(this);
		}
	}
}
