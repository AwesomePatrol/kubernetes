/*
Copyright 2025 The Kubernetes Authors.

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

package etcd3

import (
	"context"
	"time"

	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/component-base/tracing"
)

func NewKubernetesEtcdLatencyTracker(delegate kubernetes.Interface) kubernetes.Interface {
	return &kubernetesEtcdLatencyTracker{Interface: delegate}
}

type kubernetesEtcdLatencyTracker struct {
	kubernetes.Interface
}

func (k *kubernetesEtcdLatencyTracker) Get(ctx context.Context, key string, opts kubernetes.GetOptions) (kubernetes.GetResponse, error) {
	ctx, span := tracing.Start(ctx, "Get kubernetesEtcd",
		attribute.String("key", key), attribute.Int("rev", int(opts.Revision)))
	defer span.End(500 * time.Millisecond)
	return k.Interface.Get(ctx, key, opts)
}

func (k *kubernetesEtcdLatencyTracker) List(ctx context.Context, prefix string, opts kubernetes.ListOptions) (kubernetes.ListResponse, error) {
	ctx, span := tracing.Start(ctx, "List kubernetesEtcd",
		attribute.String("key", prefix), attribute.Int("rev", int(opts.Revision)), attribute.Int("limit", int(opts.Limit)))
	defer span.End(500 * time.Millisecond)
	return k.Interface.List(ctx, prefix, opts)
}

func (k *kubernetesEtcdLatencyTracker) Count(ctx context.Context, prefix string, opts kubernetes.CountOptions) (int64, error) {
	ctx, span := tracing.Start(ctx, "Count kubernetesEtcd",
		attribute.String("key", prefix))
	defer span.End(500 * time.Millisecond)
	return k.Interface.Count(ctx, prefix, opts)
}

func (k *kubernetesEtcdLatencyTracker) OptimisticPut(ctx context.Context, key string, value []byte, expectedRevision int64, opts kubernetes.PutOptions) (kubernetes.PutResponse, error) {
	ctx, span := tracing.Start(ctx, "OptimisticPut kubernetesEtcd",
		attribute.String("key", key), attribute.Int("rev", int(expectedRevision)), attribute.Int("lease", int(opts.LeaseID)), attribute.Bool("get_on_failure", opts.GetOnFailure))
	defer span.End(500 * time.Millisecond)
	return k.Interface.OptimisticPut(ctx, key, value, expectedRevision, opts)
}

func (k *kubernetesEtcdLatencyTracker) OptimisticDelete(ctx context.Context, key string, expectedRevision int64, opts kubernetes.DeleteOptions) (kubernetes.DeleteResponse, error) {
	ctx, span := tracing.Start(ctx, "OptimisticDelete kubernetesEtcd",
		attribute.String("key", key), attribute.Int("rev", int(expectedRevision)), attribute.Bool("get_on_failure", opts.GetOnFailure))
	defer span.End(500 * time.Millisecond)
	return k.Interface.OptimisticDelete(ctx, key, expectedRevision, opts)
}
