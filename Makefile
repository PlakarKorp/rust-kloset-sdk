# Makefile

.PHONY: all build run test clean

all: build

build:
	cargo build

run:
	cargo run

test:
	cargo test

clean:
	cargo clean

# Generate Protobuf files
# NOT ACTUALLY USED, BUT LEFT FOR REFERENCE, have a look at the build.rs file to see how the protobuf files are generated
PROTOS = importer exporter storage

gen:
	for proto in $(PROTOS); do \
		cp proto/$$proto.proto .; \
		mkdir -p ./pkg/$$proto/; \
		protoc \
			--proto_path=. \
			--prost_out=./pkg/$$proto/ \
			--tonic_out=./pkg/$$proto/ \
			./$$proto.proto; \
		rm -f ./$$proto.proto; \
		#mkdir -p ./connectors/grpc/$$proto/pkg/; \
		#mv ./pkg/$$proto/* ./connectors/grpc/$$proto/pkg/; \
		#rm -rf ./pkg/$$proto; \
	done
	#rm -rf ./pkg