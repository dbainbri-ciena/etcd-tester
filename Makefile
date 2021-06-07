# Copyright 2021 Ciena Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

KEYS?=0

ifeq (${KEYS},0)
KEYS_SMALL=10
KEYS_MEDIUM=1000
KEYS_MEDIUM_LARGE=1000
KEYS_LARGE=100000
else
KEYS_SMALL=${KEYS}
KEYS_MEDIUM=${KEYS}
KEYS_MEDIUM_LARGE=${KEYS}
KEYS_LARGE=${KEYS}
endif

.DEFAULT_GOAL:=help
.PHONY: help
help:  ## Display this help
	@echo "Usage: make \033[36m<target>\033[0m"
	@awk 'BEGIN {FS = ":.*## *"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "\033[36m  %s\033[0m,%s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST) | column -s , -c 2 -tx

prereqs: ## Helper commands to install things you need (ubuntu)
	sudo apt install -y make
	sudo snap install --classic go
	sudo snap install --classic docker
	sudo snap install --classic jq
	sudo snap install --classic kubectl
	sudo snap install --classic helm
	sudo usermod -aG docker ${USER}
	@echo '# You will need to log out and log in again for group setting to take'
	@echo '# then execute the following commands:'
	@echo ''
	@echo 'export PATH=$$(go env GOPATH)/bin:$$PATH'

build: ## Build the etcd tester client
	(cd client; go build -o ../etcdtester ./main.go)

clean: ## Delete build artifacts
	rm -rf ./etcdtester

kind: ## install kind
	go get sigs.k8s.io/kind

cluster-up: ## Create the kind cluster for testing
	kind create cluster --config kind-cluster.yaml --name etcd

cluster-down: ## Delete the kind cluster
	kind delete cluster --name etcd

bitnami-charts: ## Add bitnami charts as a helm repository
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update

etcd-up: ## install etcd
	helm upgrade my-release bitnami/etcd --install --values etcd-values.yaml --wait

etcd-down: ## tear down etcd
	helm delete my-release

up: cluster-up etcd-up ## create cluster and start etcd

down: cluster-down ## delete everything

defrag: ## defragment the cluster
	kubectl exec -ti my-release-etcd-0 -- etcdctl defrag --cluster --command-timeout=10s

defrag-last: ## compact the cluster to the latest revision and defragment
	kubectl exec my-release-etcd-0 -- etcdctl compact $$(kubectl exec  my-release-etcd-0 -- etcdctl endpoint status -w json | jq '.[0].Status.header.revision')
	kubectl exec -ti my-release-etcd-0 -- etcdctl defrag --cluster --command-timeout=10s

db-size: ## display the DB size
	@echo "$$(kubectl exec -ti my-release-etcd-0 -- etcdctl endpoint status -w json | jq -r '.[0].Status.dbSize' | head -1 | numfmt --to=iec-i) $$(kubectl exec -ti my-release-etcd-1 -- etcdctl endpoint status -w json | jq -r '.[0].Status.dbSize' | head -1 | numfmt --to=iec-i) $$(kubectl exec -ti my-release-etcd-2 -- etcdctl endpoint status -w json | jq -r '.[0].Status.dbSize' | head -1 | numfmt --to=iec-i)"

forwards-up:
	kubectl port-forward pod/my-release-etcd-0 2379:2379 &
	kubectl port-forward pod/my-release-etcd-1 2479:2379 &
	kubectl port-forward pod/my-release-etcd-2 2579:2379 &

forwards-down:
	pkill kubectl

test-small: ## 10 keys - 10 workers - 1000 puts
	@./etcdtester \
		--data ./client/data.json \
		--defrag 5m \
		--defrag-timeout 10s \
		--endpoints localhost:2379,localhost:2479,localhost:2579 \
		--etcd-timeout 10s \
		--keys ${KEYS_SMALL} \
		--puts 10/1s \
		--putters 1 \
		--gets 10/1s \
		--getters 1 \
		--dels 10/1s \
		--dellers 1 \
		--pcon 50 --gcon 50 --dcon 50 \
		--report 500ms ${HUMAN} ${OVERLAP}

test-medium: ## 1000 keys - 10 workers - 10000 puts
	@./etcdtester \
		--data ./client/data.json \
		--defrag 5m \
		--defrag-timeout 10s \
		--endpoints localhost:2379,localhost:2479,localhost:2579 \
		--etcd-timeout 10s \
		--keys ${KEYS_MEDIUM} \
		--puts 999999/5s \
		--putters 10 \
		--pcon 10000 \
		--gets 999999/5s \
		--getters 10 \
		--gcon 10000 \
		--dels 999999/5s \
		--dellers 10 \
		--dcon 10000 \
		--report 5s ${HUMAN} ${OVERLAP}

test-medium-large: ## 1000 keys - 10 workers - 100000 puts
	@./etcdtester \
		--data ./client/data.json \
		--defrag 5m \
		--defrag-timeout 10s \
		--endpoints localhost:2379,localhost:2479,localhost:2579 \
		--etcd-timeout 10s \
		--keys ${KEYS_MEDIUM_LARGE} \
		--puts 999999/5s \
		--putters 10 \
		--pcon 100000 \
		--gets 999999/5s \
		--getters 10 \
		--gcon 100000 \
		--dels 999999/5s \
		--dellers 10 \
		--dcon 100000 \
		--report 5s ${HUMAN} ${OVERLAP}

test-large: ## 100000 keys - 10 workers - 500000 puts
	@./etcdtester \
		--data ./client/data.json \
		--defrag 5m \
		--defrag-timeout 10s \
		--endpoints localhost:2379,localhost:2479,localhost:2579 \
		--etcd-timeout 10s \
		--keys ${KEYS_LARGE} \
		--puts 999999/5s \
		--putters 10 \
		--pcon 500000 \
		--gets 999999/5s \
		--getters 10 \
		--gcon 500000 \
		--dels 999999/5s \
		--dellers 10 \
		--dcon 500000 \
		--report 5s ${HUMAN} ${OVERLAP}

wipe: ## remove all keys from the DB
	kubectl exec -ti my-release-etcd-0 -- etcdctl del "" --from-key=true

csv-clean: ## Delete any csv files
	rm -rf *.csv
