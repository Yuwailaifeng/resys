CC = gcc
#Using -Ofast instead of -O3 might result in faster code, but is supported only by newer GCC versions
CFLAGS = -lm -pthread -O3 -march=native -Wall -funroll-loops -Wno-unused-result

all: item2vec
item2vec : item2vec.c
	$(CC) item2vec.c -o item2vec $(CFLAGS)

clean:
	rm -rf item2vec