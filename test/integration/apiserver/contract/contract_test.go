//go:build !windows
// +build !windows

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

package contract

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	traceservice "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	client "k8s.io/client-go/kubernetes"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	configmaptest "k8s.io/kubernetes/test/integration/configmap"
	"k8s.io/kubernetes/test/integration/framework"
)

func TestEtcdContractCompliance(t *testing.T) {
	// Listen for traces from the API Server before starting it, so the
	// API Server will successfully connect right away during the test.
	listener, err := net.Listen("tcp", "localhost:")
	if err != nil {
		t.Fatal(err)
	}
	// Write the configuration for tracing to a file
	tracingConfigFile, err := os.CreateTemp("", "tracing-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.Remove(tracingConfigFile.Name()); err != nil {
			t.Error(err)
		}
	}()

	if err := os.WriteFile(tracingConfigFile.Name(), []byte(fmt.Sprintf(`
apiVersion: apiserver.config.k8s.io/v1beta1
kind: TracingConfiguration
endpoint: %s`, listener.Addr().String())), os.FileMode(0755)); err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	fakeServer := &traceServer{t: t}
	fakeServer.resetExpectations([]*spanExpectation{}, trace.TraceID{})
	traceservice.RegisterTraceServiceServer(srv, fakeServer)

	go func() {
		if err = srv.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	defer srv.Stop()

	// Start the API Server with our tracing configuration
	testServer := kubeapiservertesting.StartTestServerOrDie(t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{"--tracing-config-file=" + tracingConfigFile.Name()},
		framework.SharedEtcd(),
	)
	defer testServer.TearDownFn()
	clientSet, err := client.NewForConfig(testServer.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		desc          string
		apiCall       func(context.Context) error
		expectedTrace []*spanExpectation
	}{
		{
			desc: "configmap",
			apiCall: func(ctx context.Context) error {
				ns := framework.CreateNamespaceOrDie(clientSet, "config-map", t)
				defer framework.DeleteNamespaceOrDie(clientSet, ns, t)
				configmaptest.DoTestConfigMap(ctx, t, clientSet, ns)
				return nil
			},
			expectedTrace: []*spanExpectation{
				{
					name: "OptimisticPut kubernetesEtcdContract",
					attributes: map[string]func(*commonv1.AnyValue) bool{
						"key": func(v *commonv1.AnyValue) bool {
							return strings.Contains(v.GetStringValue(), "/configmaps/")
						},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, testTraceID := sampledContext()
			fakeServer.resetExpectations(tc.expectedTrace, testTraceID)

			// Make our call to the API server
			if err := tc.apiCall(ctx); err != nil {
				t.Fatal(err)
			}

			// Wait for a span to be recorded from our request
			select {
			case <-fakeServer.traceFound:
			case <-time.After(30 * time.Second):
				for _, spanExpectation := range fakeServer.expectations {
					if !spanExpectation.met {
						t.Logf("Unmet expectation: %s", spanExpectation.name)
					}
				}
				t.Fatal("Timed out waiting for trace")
			}
		})
	}
}

// traceServer implements TracesServiceServer, which can receive spans from the
// API Server via OTLP.
type traceServer struct {
	t *testing.T
	traceservice.UnimplementedTraceServiceServer
	// the lock guards the per-scenario state below
	lock         sync.Mutex
	traceFound   chan struct{}
	expectations traceExpectation
	testTraceID  trace.TraceID
}

func (t *traceServer) Export(ctx context.Context, req *traceservice.ExportTraceServiceRequest) (*traceservice.ExportTraceServiceResponse, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.expectations.update(req, t.testTraceID)
	// if all expectations are met, notify the test scenario by closing traceFound
	if t.expectations.met() {
		select {
		case <-t.traceFound:
			// traceFound is already closed
		default:
			close(t.traceFound)
		}
	}
	return &traceservice.ExportTraceServiceResponse{}, nil
}

// resetExpectations is used by a new test scenario to set new expectations for
// the test server.
func (t *traceServer) resetExpectations(newExpectations traceExpectation, traceID trace.TraceID) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.traceFound = make(chan struct{})
	t.expectations = newExpectations
	t.testTraceID = traceID
}

// traceExpectation is an expectation for an entire trace
type traceExpectation []*spanExpectation

// met returns true if all span expectations the server is looking for have
// been satisfied.
func (t traceExpectation) met() bool {
	for _, se := range t {
		if !se.met {
			return false
		}
	}
	return true
}

// update finds all expectations that are met by a span in the
// incoming request.
func (t traceExpectation) update(req *traceservice.ExportTraceServiceRequest, traceID trace.TraceID) {
	for _, resourceSpans := range req.GetResourceSpans() {
		for _, instrumentationSpans := range resourceSpans.GetScopeSpans() {
			for _, span := range instrumentationSpans.GetSpans() {
				t.updateForSpan(span, traceID)
			}
		}
	}
}

// updateForSpan updates expectations based on a single incoming span.
func (t traceExpectation) updateForSpan(span *tracev1.Span, traceID trace.TraceID) {
	if hex.EncodeToString(span.TraceId) != traceID.String() {
		return
	}
	for i, spanExpectation := range t {
		if spanExpectation.name != "" && span.Name != spanExpectation.name {
			continue
		}
		if !spanExpectation.attributes.matches(span.GetAttributes()) {
			continue
		}
		if !spanExpectation.events.matches(span.GetEvents()) {
			continue
		}
		t[i].met = true
	}

}

// spanExpectation is the expectation for a single span
type spanExpectation struct {
	name       string
	attributes attributeExpectation
	events     eventExpectation
	met        bool
}

// eventExpectation is the expectation for an event attached to a span.
// It is comprised of event names.
type eventExpectation []string

// matches returns true if all expected events exist in the list of input events.
func (e eventExpectation) matches(events []*tracev1.Span_Event) bool {
	eventMap := map[string]struct{}{}
	for _, event := range events {
		eventMap[event.Name] = struct{}{}
	}
	for _, wantEvent := range e {
		if _, ok := eventMap[wantEvent]; !ok {
			return false
		}
	}
	return true
}

// eventExpectation is the expectation for an event attached to a span.
// It is a map from attribute key, to a value-matching function.
type attributeExpectation map[string]func(*commonv1.AnyValue) bool

// matches returns true if all expected attributes exist in the intput list of attributes.
func (a attributeExpectation) matches(attrs []*commonv1.KeyValue) bool {
	attrsMap := map[string]*commonv1.AnyValue{}
	for _, attr := range attrs {
		attrsMap[attr.GetKey()] = attr.GetValue()
	}
	for key, checkVal := range a {
		if val, ok := attrsMap[key]; !ok || !checkVal(val) {
			return false
		}
	}
	return true
}

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func sampledContext() (context.Context, trace.TraceID) {
	tid := trace.TraceID{}
	_, _ = r.Read(tid[:])
	sid := trace.SpanID{}
	_, _ = r.Read(sid[:])
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(context.Background(), sc), tid
}
