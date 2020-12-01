# nifi-scripting-patterns
scripting patterns that let build processor for InvokeSriptedProcessor in easy way

**WARNING**: not working project, just a concept

simple example of idea

		Function<InputStream, Supplier<Map<String, Object>>> JSON_BY_LINE_READER = (InputStream in) -> ()-> Map.of("key", "value");
		Function<Map<String, Object>, byte[]> JSON_BY_LINE_WRITER = map -> map.toString().getBytes();
		Function<Map<String, Object>, Map<String, Object>> transform = map -> {
			map.put("key", map.get("key") + "jotaro");
			return map;
		};
		Processor p = StandardProcessor.of(JSON_BY_LINE_READER, JSON_BY_LINE_WRITER)
						.withTransformation(Transformation.of(transform)
						.withRelationship("DDS"));
