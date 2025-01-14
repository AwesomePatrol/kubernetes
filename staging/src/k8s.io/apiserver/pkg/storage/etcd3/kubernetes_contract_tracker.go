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

	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationScope = "k8s.io/apiserver/pkg/storage/etcd3"

func NewKubernetesEtcdContractTracker(delegate kubernetes.Interface, tp trace.TracerProvider) kubernetes.Interface {
	return &kubernetesEtcdContractTracker{Interface: delegate, tracer: tp.Tracer(instrumentationScope)}
}

type kubernetesEtcdContractTracker struct {
	kubernetes.Interface

	tracer trace.Tracer
}

func (k *kubernetesEtcdContractTracker) Get(ctx context.Context, key string, opts kubernetes.GetOptions) (kubernetes.GetResponse, error) {
	ctx, span := k.tracer.Start(ctx, "Get kubernetesEtcdContract",
		trace.WithAttributes(attribute.String("key", key), attribute.Int("rev", int(opts.Revision))))
	defer span.End()
	return k.Interface.Get(ctx, key, opts)
}

func (k *kubernetesEtcdContractTracker) List(ctx context.Context, prefix string, opts kubernetes.ListOptions) (kubernetes.ListResponse, error) {
	ctx, span := k.tracer.Start(ctx, "List kubernetesEtcdContract",
		trace.WithAttributes(attribute.String("key", prefix), attribute.Int("rev", int(opts.Revision)), attribute.Int("limit", int(opts.Limit))))
	defer span.End()
	return k.Interface.List(ctx, prefix, opts)
}

func (k *kubernetesEtcdContractTracker) Count(ctx context.Context, prefix string, opts kubernetes.CountOptions) (int64, error) {
	ctx, span := k.tracer.Start(ctx, "Count kubernetesEtcdContract",
		trace.WithNewRoot(), // Count is called periodically from the same context, so it would show up as a single trace otherwise
		trace.WithAttributes(attribute.String("key", prefix)))
	defer span.End()
	return k.Interface.Count(ctx, prefix, opts)
}

func (k *kubernetesEtcdContractTracker) OptimisticPut(ctx context.Context, key string, value []byte, expectedRevision int64, opts kubernetes.PutOptions) (kubernetes.PutResponse, error) {
	ctx, span := k.tracer.Start(ctx, "OptimisticPut kubernetesEtcdContract",
		trace.WithAttributes(attribute.String("key", key), attribute.Int("rev", int(expectedRevision)), attribute.Int("lease", int(opts.LeaseID)), attribute.Bool("get_on_failure", opts.GetOnFailure)))
	defer span.End()
	return k.Interface.OptimisticPut(ctx, key, value, expectedRevision, opts)
}

func (k *kubernetesEtcdContractTracker) OptimisticDelete(ctx context.Context, key string, expectedRevision int64, opts kubernetes.DeleteOptions) (kubernetes.DeleteResponse, error) {
	ctx, span := k.tracer.Start(ctx, "OptimisticDelete kubernetesEtcdContract",
		trace.WithAttributes(attribute.String("key", key), attribute.Int("rev", int(expectedRevision)), attribute.Bool("get_on_failure", opts.GetOnFailure)))
	defer span.End()
	return k.Interface.OptimisticDelete(ctx, key, expectedRevision, opts)
}
