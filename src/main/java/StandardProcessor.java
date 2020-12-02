import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StandardProcessor<T,M> extends AbstractProcessor {

	List<Transformation<T,M>> transformations = new ArrayList<>();
	Function<InputStream, Supplier<T>> reader;
	Function<M, byte[]> writer;

	private List<PropertyDescriptor> propertyDescriptors;
	private Set<Relationship> relationships;
	private Relationship defaultRelationship;

	public StandardProcessor(){}

	private StandardProcessor(Builder<T,M> builder){
		this.transformations = builder.transformations;
		this.reader = builder.reader;
		this.writer = builder.writer;
		this.propertyDescriptors = builder.propertyDescriptors;
		this.relationships = builder.relationships;
		this.defaultRelationship = builder.defaultRelationship;
	}

	protected StandardProcessor(Function<InputStream, Supplier<T>> reader, Function<M, byte[]> writer){
		this.reader = reader;
		this.writer = writer;
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return propertyDescriptors;
	}

	public StandardProcessor withReader(Function<InputStream, Supplier<T>> reader){
		this.reader = reader;
		return this;
	}

	public String toString(){
		return "kek";
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		LinkedList<FlowFile> flowFiles = new LinkedList<>(session.get(10));
		List<Pipeline<T,M>> pipelines = transformations.stream()
				.collect(Collectors.groupingBy(transformation->Optional.ofNullable(transformation.getRelationship())))
				.entrySet().stream()
				.map(entry -> Pipeline.of(entry.getValue(),
						writer,
						session.create(),
						entry.getKey().or(()->Optional.ofNullable(defaultRelationship)),
						session,
						getLogger())
				)
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

	public static class Builder<T,M>{

		private List<Transformation<T,M>> transformations = new ArrayList<>();
		private Function<InputStream, Supplier<T>> reader;
		private Function<M, byte[]> writer;
		private List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
		private Set<Relationship> relationships = new HashSet<>();
		private Relationship defaultRelationship;

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

		public Builder<T,M> withDescriptor(PropertyDescriptor property){
			propertyDescriptors.add(property);
			return this;
		}

		public Builder<T,M> withDefaultRelationship(Relationship relationship){
			relationships.add(relationship);
			this.defaultRelationship = relationship;
			return this;
		}

		public <K> Builder<T,K> withWriter(Function<K, byte[]> writer){
			return new Builder<T,K>(reader, writer);
		}

		public <K> Builder<K,M> withReader(Function<InputStream, Supplier<K>> reader){
			return new Builder<>(reader, writer);
		}

		public Builder<T,M> withTransformation(Transformation<T,M> transformation){
			transformations.add(transformation);
			if (transformation.getRelationship()!=null){
				relationships.add(transformation.getRelationship());
			}
			return new Builder<>(this);
		}

		public StandardProcessor<T,M> build(){
			return new StandardProcessor<>(this);
		}
	}
}
