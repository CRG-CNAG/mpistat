import proto3_pb2 as mpistat

from google.protobuf.internal.encoder import _VarintBytes
from google.protobuf.internal.decoder import _DecodeVarint32


def main():
    '''
    main entry point
    '''
    with gzip.open(sys.argv[1], 'rb') as f:
        buf = f.read(10)
        while buf:
            msg_len, new_pos = _DecodeVarint32(buf,0)
            buf = buf[new_pos:]
            buf += f.read(msg_len - len(buf))
            msg = mpistat.MpistatMsg()
            msg.ParseFromString(buf)
            print(msg)
            buf = f.read(10)

if __name__ == '__main__':
    sys.exit(main())
