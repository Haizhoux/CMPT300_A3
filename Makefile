all:
	gcc -pthread -o myChannels myChannels.c -lm
clean:
	rm -f myChannels