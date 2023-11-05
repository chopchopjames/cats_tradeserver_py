#!/bin/bash
python -m grpc.tools.protoc -I../proto --python_out=. ../proto/quotation.proto
python -m grpc.tools.protoc -I../proto --python_out=. ../proto/trade.proto