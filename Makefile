build:
	gcc main.c -g -o tema1 -lpthread  -Wall
build_debug:
	gcc main.c -g -o tema1 -lpthread -DDEBUG -g3 -O0 -Werror -Wall	
clean:
	rm -rf tema1