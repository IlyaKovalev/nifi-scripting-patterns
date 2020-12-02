import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

class Pipeline<T,M> {

	private List<Transformation<T, M>> transformations;
	private FlowFile file;
	private Optional<Relationship> relationship;
	private OutputStream out;
	private ProcessSession session;
	private Function<M, byte[]> writer;
	private ComponentLog log;
	private Map<String, String> attributes;

	public Pipeline(List<Transformation<T, M>> transformations,
	                Function<M, byte[]> writer,
	                FlowFile file,
	                Optional<Relationship> relationship,
	                ProcessSession session,
	                ComponentLog log){
		this.relationship = relationship;
		this.transformations = transformations;
		this.writer = writer;
		this.file = file;
		this.session = session;
		this.log = log;
	}

	public static <T,M> Pipeline<T,M> of(List<Transformation<T, M>> transformations,
										 Function<M, byte[]> writer,
										 FlowFile file,
										 Optional<Relationship> relationship,
										 ProcessSession session,
										 ComponentLog log){
		return new Pipeline<>(transformations, writer, file, relationship, session, log);
	}

	public void execute(T value, FlowFile inFlowFile) {
		for (Transformation<T,M> transformation: transformations){
			if (attributes==null){
				attributes = inFlowFile.getAttributes();
			}
			M result = transformation.execute(value, attributes);
			if (result!=null && writer!=null){
				if (out==null){
					out = new BufferedOutputStream(session.write(file));
				}
				try {
					out.write(writer.apply(result));
				} catch (IOException e) {
					log.error("ds");
				}
			}
		}
	}

	public void close() {
		if (out==null){
			return;
		}
		try {
			out.close();
		} catch (IOException e) {
			log.error("TODO");
		}

		if (relationship.isPresent()){
			session.putAllAttributes(file, attributes);
			session.transfer(file, this.relationship.get());
		}else {
			session.remove(file);
		}

	}
}
