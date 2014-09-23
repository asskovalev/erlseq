app:
	./rebar compile -j 10 skip_deps=true

test: eunit

eunit:
	./rebar eunit skip_deps=true

clean:
	./rebar clean