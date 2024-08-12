package operator

import (
	"reflect"
	"testing"

	"github.com/openshift/aws-efs-csi-driver-operator/assets"
	"github.com/openshift/library-go/pkg/operator/resource/resourceread"
	appsv1 "k8s.io/api/apps/v1"
)

func getTestDeployment() *appsv1.Deployment {
	data, err := assets.ReadFile("controller.yaml")
	if err != nil {
		panic(err)
	}
	return resourceread.ReadDeploymentV1OrDie(data)
}

func getTestDaemonSet() *appsv1.DaemonSet {
	data, err := assets.ReadFile("node.yaml")
	if err != nil {
		panic(err)
	}
	return resourceread.ReadDaemonSetV1OrDie(data)
}

func Test_WithFIPSDeploymentHook(t *testing.T) {
	tests := []struct {
		name            string
		fipsEnabled     string
		expectedEnvVars map[string]string
	}{
		{
			name:        "FIPS enabled",
			fipsEnabled: "true",
			expectedEnvVars: map[string]string{
				"FIPS_ENABLED": "true",
			},
		},
		{
			name:        "FIPS disabled",
			fipsEnabled: "false",
			expectedEnvVars: map[string]string{
				"FIPS_ENABLED": "false",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := getTestDeployment()

			// Act
			err := withFIPSDeploymentHookInternal(tt.fipsEnabled)(nil, deployment)
			if err != nil {
				t.Fatalf("unexpected hook error: %v", err)
			}

			// Assert
			found := false
			for _, container := range deployment.Spec.Template.Spec.Containers {
				if container.Name == "csi-driver" {
					// Collect FIPS_ENABLED from struct EnvVar to map[string]string
					containerEnvs := map[string]string{}
					for _, env := range container.Env {
						if env.Name == "FIPS_ENABLED" {
							containerEnvs[env.Name] = env.Value
						}
					}
					if !reflect.DeepEqual(containerEnvs, tt.expectedEnvVars) {
						t.Errorf("expected csi-driver env var %+v, got %+v", tt.expectedEnvVars, containerEnvs)
					}
					found = true
					break
				}
			}
			if !found {
				t.Errorf("container csi-driver not found")
			}
		})
	}
}

func Test_WithFIPSDaemonSetHook(t *testing.T) {
	tests := []struct {
		name            string
		fipsEnabled     string
		expectedEnvVars map[string]string
	}{
		{
			name:        "FIPS enabled",
			fipsEnabled: "true",
			expectedEnvVars: map[string]string{
				"FIPS_ENABLED": "true",
			},
		},
		{
			name:        "FIPS disabled",
			fipsEnabled: "false",
			expectedEnvVars: map[string]string{
				"FIPS_ENABLED": "false",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			daemonSet := getTestDaemonSet()

			// Act
			err := withFIPSDaemonSetHookInternal(tt.fipsEnabled)(nil, daemonSet)
			if err != nil {
				t.Fatalf("unexpected hook error: %v", err)
			}

			// Assert
			found := false
			for _, container := range daemonSet.Spec.Template.Spec.Containers {
				if container.Name == "csi-driver" {
					// Collect FIPS_ENABLED from struct EnvVar to map[string]string
					containerEnvs := map[string]string{}
					for _, env := range container.Env {
						if env.Name == "FIPS_ENABLED" {
							containerEnvs[env.Name] = env.Value
						}
					}
					if !reflect.DeepEqual(containerEnvs, tt.expectedEnvVars) {
						t.Errorf("expected csi-driver env var %+v, got %+v", tt.expectedEnvVars, containerEnvs)
					}
					found = true
					break
				}
			}
			if !found {
				t.Errorf("container csi-driver not found")
			}
		})
	}
}
