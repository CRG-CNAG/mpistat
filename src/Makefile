CFLAGS=-O2 -std=gnu++14 
LIBS=-lz -L${CIRCLE_HOME}/lib -lcircle -L${BOOST_HOME}/lib -lboost_iostreams -lboost_system -L${PROTOBUF_HOME}/lib -lprotobuf
INCLUDE=-I${CIRCLE_HOME}/include -I${BOOST_HOME}/include -I${PROTOBUF_HOME}/include
all : ../bin/mpistat ../bin/proto3_pb2.py

mpistat.proto3.pb.cc : mpistat.proto3
	protoc -I=. --cpp_out=. mpistat.proto3

../bin/mpistat : mpistat.cc FileSuffix.hpp mpistat.proto3.pb.cc
	mpic++ $(CFLAGS) -o ../bin/mpistat mpistat.cc mpistat.proto3.pb.cc $(INCLUDE) $(LIBS)

../bin/proto3_pb2.py:
    protoc mpistat.proto3 --python_out=.
    mv mpistat/proto3_pb2.py ../bin/
    rmdir mpistat

clean :
	rm ../bin/mpistat ../bin/proto3_pb2.py
