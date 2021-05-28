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

.DEFAULT_GOAL:=help
.PHONY: help
help:  ## Display this help
	@echo "Usage: make \033[36m<target>\033[0m"
	@awk 'BEGIN {FS = ":.*## *"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "\033[36m  %s\033[0m,%s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST) | column -s , -c 2 -tx

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

etcd-defrag: ## defragment the cluster
	kubectl exec -ti my-release-etcd-0 -- etcdctl defrag --cluster --command-timeout=10s

etcd-defrag-last: ## compact the cluster to the latest revision and defragment
	kubectl exec my-release-etcd-0 -- etcdctl compact $$(kubectl exec  my-release-etcd-0 -- etcdctl endpoint status -w json | jq '.[0].Status.header.revision')
	kubectl exec -ti my-release-etcd-0 -- etcdctl defrag --cluster --command-timeout=10s

etcd-db-size: ## display the DB size
	@echo "$$(kubectl exec -ti my-release-etcd-0 -- etcdctl endpoint status -w json | jq -r '.[0].Status.dbSize' | head -1 | numfmt --to=iec-i) $$(kubectl exec -ti my-release-etcd-1 -- etcdctl endpoint status -w json | jq -r '.[0].Status.dbSize' | head -1 | numfmt --to=iec-i) $$(kubectl exec -ti my-release-etcd-2 -- etcdctl endpoint status -w json | jq -r '.[0].Status.dbSize' | head -1 | numfmt --to=iec-i)"

wipe: ## remove all keys from the DB
	kubectl exec -ti my-release-etcd-0 -- etcdctl del "" --from-key=true
