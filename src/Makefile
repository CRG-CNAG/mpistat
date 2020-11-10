CFLAGS=-O2 -Wall -std=gnu++14 -Wall -Weffc++
LIBS=-lz -L${CIRCLE_HOME}/lib -lcircle -L${BOOST_HOME}/lib -lboost_iostreams -lboost_system -L${PROTOBUF_HOME}/lib -lprotobuf

all : ../bin/mpistat 

mpistat.proto3.pb.cc : mpistat.proto3
	protoc -I=. --cpp_out=. mpistat.proto3

../bin/mpistat : mpistat.cc FileSuffix.hpp mpistat.proto3.pb.cc
	mpic++ $(CFLAGS) -o ../bin/mpistat mpistat.cc mpistat.proto3.pb.cc $(LIBS)

clean :
	rm ../bin/mpistat
