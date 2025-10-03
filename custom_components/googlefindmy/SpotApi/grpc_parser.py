#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
import struct
import gzip
import io

class GrpcParser:
    @staticmethod
    def extract_grpc_payload(grpc: bytes) -> bytes:
        # Defensive guards for gRPC length-prefixed frame: 1 byte flag + 4 bytes length
        if not grpc or len(grpc) < 5:
            raise ValueError("Invalid GRPC payload (too short for frame header)")

        flag = grpc[0]  # 0 = uncompressed, 1 = compressed (gzip by default)
        if flag not in (0, 1):
            raise ValueError(f"Invalid GRPC payload (bad compressed-flag {flag})")

        length = struct.unpack(">I", grpc[1:5])[0]
        if len(grpc) < 5 + length:
            raise ValueError(f"Invalid GRPC payload length (expected {length}, got {len(grpc) - 5})")

        # Extract exactly one message frame (unary RPC)
        msg = grpc[5:5 + length]

        if flag == 1:
            # Compressed frame (gzip) → decompress
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(msg)) as gz:
                    return gz.read()
            except Exception as e:
                raise ValueError(f"Failed to decompress gRPC frame: {e}")

        return msg

    @staticmethod
    def construct_grpc(payload: bytes) -> bytes:
        # not compressed
        compressed = bytes([0])
        length = len(payload)
        length_data = struct.pack(">I", length)
        return compressed + length_data + payload
