import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class Pipeline<T,M> {

	private List<Transformation<T, M>> transformations;
	private FlowFile file;
	private OutputStream out;
	private ProcessSession session;
	private Function<M, byte[]> writer;
	private ComponentLog log;
	private Map<String, String> attributes;

	public Pipeline(List<Transformation<T, M>> transformations,
	                Function<M, byte[]> writer,
	                FlowFile file,
	                ProcessSession session,
	                ComponentLog log){
		this.transformations = transformations;
		this.writer = writer;
		this.file = session.clone(file);
		this.session = session;
		this.log = log;
	}

	public static <T,M> Pipeline<T,M> of(List<Transformation<T, M>> transformations,
	                          Function<M, byte[]> writer,
	                          FlowFile file,
	                          ProcessSession session,
	                          ComponentLog log){
		return new Pipeline<>(transformations, writer, file, session, log);
	}

	public void execute(T value, FlowFile inFlowFile) {
		for (Transformation<T,M> transformation: transformations){
			if (attributes==null){
				attributes = inFlowFile.getAttributes();
			}
			M result = transformation.execute(value, attributes);
			if (result!=null){
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
		session.putAllAttributes(file, attributes);
		session.transfer(file);
	}
}
