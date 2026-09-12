server:main.cc
	g++ -o $@ $^ -std=c++20 -I/usr/include/jsoncpp -levent -levent_pthreads -lmysqlclient -g -ljsoncpp
.PHONY:clean
clean:
	rm -f server