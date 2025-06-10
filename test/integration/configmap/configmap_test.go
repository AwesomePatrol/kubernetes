/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package configmap

// This file tests use of the configMap API resource.

import (
	"testing"

	clientset "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
)

// TestConfigMap tests apiserver-side behavior of creation of ConfigMaps and pods that consume them.
func TestConfigMap(t *testing.T) {
	// Disable ServiceAccount admission plugin as we don't have serviceaccount controller running.
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	defer server.TearDownFn()

	client := clientset.NewForConfigOrDie(server.ClientConfig)

	ns := framework.CreateNamespaceOrDie(client, "config-map", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	DoTestConfigMap(t.Context(), t, client, ns)
}
