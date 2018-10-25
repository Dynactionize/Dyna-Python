CURRENT_DIR := $(shell pwd)

GATEWAY_REPO := http://bitbucket.dyna:7990/scm/go/dynagatewaytypes.git

all: init grpc

clean: clean_grpc

init:
	pip3 install -r requirements.txt

	-rm -rf $(CURRENT_DIR)/deps/grpc
	-mkdir -p $(CURRENT_DIR)/deps
	cd $(CURRENT_DIR)/deps && git clone --recursive https://github.com/grpc/grpc
	cd $(CURRENT_DIR)/deps/grpc && make plugins -j 12

grpc: proto
	cd proto/dynagatewaytypes && make python PYTHON_PREFIX=$(CURRENT_DIR)/dyna

clean_grpc:
	-rm -rf proto
	-rm -rf $(CURRENT_DIR)/dyna/dynagatewaytypes	

proto:
	-mkdir -p proto
	cd proto && git clone $(GATEWAY_REPO)


.PHONY: init test grpc

