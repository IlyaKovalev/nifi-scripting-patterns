import org.apache.nifi.processor.Relationship;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Transformation<E,T> {

	private BiFunction<E, Map<String, String>, T> transform;
	public static Relationship SUCCESS = new Relationship.Builder().name("SUCCESS").build();
	private Relationship relationship = SUCCESS;


	protected Transformation(BiFunction<E, Map<String, String>, T> transform){
		this.transform = transform;
	}

	public T execute(E value, Map<String, String> attributes){
		return transform.apply(value, attributes);
	}

	public static <E,T> Transformation<E,T> of(BiFunction<E, Map<String, String>, T> transform){
		return new Transformation<>(transform);
	}

	public static <E,T> Transformation<E,T> of(Function<E,T> transform){
		return new Transformation<>((E e, Map<String, String> attributes)-> transform.apply(e));
	}

	public Transformation<E,T> withRelationship(Relationship relationship){
		this.relationship = relationship;
		return this;
	}

	public Transformation<E,T> withRelationship(String relationshipName){
		this.relationship =  new Relationship.Builder()
				.name(relationshipName)
				.build();
		return this;
	}


	public Relationship getRelationship() {
		return relationship;
	}
}
