package operator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/openshift/library-go/pkg/operator/v1helpers"

	"github.com/openshift/aws-efs-csi-driver-operator/assets"
	"github.com/openshift/aws-efs-csi-driver-operator/pkg/operator/staticresource"
	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/csi/csidrivercontrollerservicecontroller"
	"github.com/openshift/library-go/pkg/operator/csi/csidrivernodeservicecontroller"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/resource/resourceread"
	"k8s.io/client-go/dynamic"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	opv1 "github.com/openshift/api/operator/v1"
	configclient "github.com/openshift/client-go/config/clientset/versioned"
	configinformers "github.com/openshift/client-go/config/informers/externalversions"
	operatorv1client "github.com/openshift/client-go/operator/clientset/versioned"
	operatorinformer "github.com/openshift/client-go/operator/informers/externalversions"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/operator/csi/csicontrollerset"
	dc "github.com/openshift/library-go/pkg/operator/deploymentcontroller"
	goc "github.com/openshift/library-go/pkg/operator/genericoperatorclient"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	// Operand and operator run in the same namespace
	operatorName       = "aws-efs-csi-driver-operator"
	trustedCAConfigMap = "aws-efs-csi-driver-trusted-ca-bundle"

	namespaceReplaceKey = "${NAMESPACE}"
	// From credentials.yaml
	secretName = "aws-efs-cloud-credentials"
)

func RunOperator(ctx context.Context, controllerConfig *controllercmd.ControllerContext) error {
	operatorNamespace := controllerConfig.OperatorNamespace

	// Create core clientset and informer
	kubeClient := kubeclient.NewForConfigOrDie(rest.AddUserAgent(controllerConfig.KubeConfig, operatorName))
	kubeInformersForNamespaces := v1helpers.NewKubeInformersForNamespaces(kubeClient, operatorNamespace, "")
	secretInformer := kubeInformersForNamespaces.InformersFor(operatorNamespace).Core().V1().Secrets()
	nodeInformer := kubeInformersForNamespaces.InformersFor("").Core().V1().Nodes()
	configMapInformer := kubeInformersForNamespaces.InformersFor(operatorNamespace).Core().V1().ConfigMaps()
	typedVersionedClient := operatorv1client.NewForConfigOrDie(controllerConfig.KubeConfig)
	operatorInformer := operatorinformer.NewSharedInformerFactory(typedVersionedClient, 20*time.Minute)

	// Create config clientset and informer. This is used to get the cluster ID
	configClient := configclient.NewForConfigOrDie(rest.AddUserAgent(controllerConfig.KubeConfig, operatorName))
	configInformers := configinformers.NewSharedInformerFactory(configClient, 20*time.Minute)
	infraInformer := configInformers.Config().V1().Infrastructures()

	// Create GenericOperatorclient. This is used by the library-go controllers created down below
	gvr := opv1.SchemeGroupVersion.WithResource("clustercsidrivers")
	operatorClient, dynamicInformers, err := goc.NewClusterScopedOperatorClientWithConfigName(controllerConfig.KubeConfig, gvr, string(opv1.AWSEFSCSIDriver))
	if err != nil {
		return err
	}

	// Dynamic client for CredentialsRequest
	dynamicClient, err := dynamic.NewForConfig(controllerConfig.KubeConfig)
	if err != nil {
		return err
	}

	cs := csicontrollerset.NewCSIControllerSet(
		operatorClient,
		controllerConfig.EventRecorder,
	).WithManagementStateController(
		operatorName,
		true,
	).WithLogLevelController().WithCSIConfigObserverController(
		"AWSEFSDriverCSIConfigObserverController",
		configInformers,
	).WithCSIDriverNodeService(
		"AWSEFSDriverNodeServiceController",
		replaceNamespaceFunc(operatorNamespace),
		"node.yaml",
		kubeClient,
		kubeInformersForNamespaces.InformersFor(operatorNamespace),
		nil,
		csidrivernodeservicecontroller.WithCABundleDaemonSetHook(
			operatorNamespace,
			trustedCAConfigMap,
			configMapInformer,
		),
		withFIPSDaemonSetHook()).WithCSIDriverControllerService(
		"AWSEFSDriverControllerServiceController",
		replaceNamespaceFunc(operatorNamespace),
		"controller.yaml",
		kubeClient,
		kubeInformersForNamespaces.InformersFor(operatorNamespace),
		configInformers,
		[]factory.Informer{
			secretInformer.Informer(),
			nodeInformer.Informer(),
			infraInformer.Informer(),
		},
		csidrivercontrollerservicecontroller.WithCABundleDeploymentHook(
			operatorNamespace,
			trustedCAConfigMap,
			configMapInformer,
		),
		csidrivercontrollerservicecontroller.WithSecretHashAnnotationHook(operatorNamespace, secretName, secretInformer),
		csidrivercontrollerservicecontroller.WithObservedProxyDeploymentHook(),
		csidrivercontrollerservicecontroller.WithReplicasHook(nodeInformer.Lister()),
		withFIPSDeploymentHook(),
	).WithCredentialsRequestController(
		"AWSEFSDriverCredentialsRequestController",
		operatorNamespace,
		replaceNamespaceFunc(operatorNamespace),
		"credentials.yaml",
		dynamicClient,
		operatorInformer,
	).WithServiceMonitorController(
		"AWSEFSDriverServiceMonitorController",
		dynamicClient,
		replaceNamespaceFunc(operatorNamespace),
		"servicemonitor.yaml",
	)

	objsToSync := staticresource.SyncObjects{
		CSIDriver:                resourceread.ReadCSIDriverV1OrDie(mustReplaceNamespace(operatorNamespace, "csidriver.yaml")),
		PrivilegedRole:           resourceread.ReadClusterRoleV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/privileged_role.yaml")),
		NodeServiceAccount:       resourceread.ReadServiceAccountV1OrDie(mustReplaceNamespace(operatorNamespace, "node_sa.yaml")),
		NodeRoleBinding:          resourceread.ReadClusterRoleBindingV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/node_privileged_binding.yaml")),
		ControllerServiceAccount: resourceread.ReadServiceAccountV1OrDie(mustReplaceNamespace(operatorNamespace, "controller_sa.yaml")),
		ControllerRoleBinding:    resourceread.ReadClusterRoleBindingV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/controller_privileged_binding.yaml")),
		ProvisionerRole:          resourceread.ReadClusterRoleV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/provisioner_role.yaml")),
		ProvisionerRoleBinding:   resourceread.ReadClusterRoleBindingV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/provisioner_binding.yaml")),
		PrometheusRole:           resourceread.ReadRoleV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/prometheus_role.yaml")),
		PrometheusRoleBinding:    resourceread.ReadRoleBindingV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/prometheus_rolebinding.yaml")),
		MetricsService:           resourceread.ReadServiceV1OrDie(mustReplaceNamespace(operatorNamespace, "service.yaml")),
		RBACProxyRole:            resourceread.ReadClusterRoleV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/kube_rbac_proxy_role.yaml")),
		RBACProxyRoleBinding:     resourceread.ReadClusterRoleBindingV1OrDie(mustReplaceNamespace(operatorNamespace, "rbac/kube_rbac_proxy_binding.yaml")),
		CAConfigMap:              resourceread.ReadConfigMapV1OrDie(mustReplaceNamespace(operatorNamespace, "cabundle_cm.yaml")),
	}
	staticController := staticresource.NewCSIStaticResourceController(
		"CSIStaticResourceController",
		operatorNamespace,
		operatorClient,
		kubeClient,
		kubeInformersForNamespaces,
		controllerConfig.EventRecorder,
		objsToSync,
	)

	klog.Info("Starting the informers")
	go kubeInformersForNamespaces.Start(ctx.Done())
	go dynamicInformers.Start(ctx.Done())
	go configInformers.Start(ctx.Done())
	go operatorInformer.Start(ctx.Done())

	klog.Info("Starting controllerset")
	go cs.Run(ctx, 1)
	go staticController.Run(ctx, 1)

	<-ctx.Done()

	return fmt.Errorf("stopped")
}

func mustReplaceNamespace(namespace, file string) []byte {
	content, err := assets.ReadFile(file)
	if err != nil {
		panic(err)
	}
	return bytes.Replace(content, []byte(namespaceReplaceKey), []byte(namespace), -1)
}

func replaceNamespaceFunc(namespace string) resourceapply.AssetFunc {
	return func(name string) ([]byte, error) {
		content, err := assets.ReadFile(name)
		if err != nil {
			panic(err)
		}
		return bytes.Replace(content, []byte(namespaceReplaceKey), []byte(namespace), -1), nil
	}
}

func getFIPSEnabled() string {
	content, err := os.ReadFile("/proc/sys/crypto/fips_enabled")
	if err == nil && string(content) == "1\n" {
		return "true"
	}
	return "false"
}

func withFIPSDeploymentHookInternal(fipsEnbaled string) dc.DeploymentHookFunc {
	return func(_ *opv1.OperatorSpec, deployment *appsv1.Deployment) error {
		for i := range deployment.Spec.Template.Spec.Containers {
			container := &deployment.Spec.Template.Spec.Containers[i]
			if container.Name != "csi-driver" {
				continue
			}
			container.Env = append(container.Env, corev1.EnvVar{
				Name:  "FIPS_ENABLED",
				Value: fipsEnbaled,
			})
		}
		return nil
	}
}

func withFIPSDeploymentHook() dc.DeploymentHookFunc {
	return withFIPSDeploymentHookInternal(getFIPSEnabled())
}

func withFIPSDaemonSetHookInternal(fipsEnbaled string) csidrivernodeservicecontroller.DaemonSetHookFunc {
	return func(_ *opv1.OperatorSpec, daemonSet *appsv1.DaemonSet) error {
		for i := range daemonSet.Spec.Template.Spec.Containers {
			container := &daemonSet.Spec.Template.Spec.Containers[i]
			if container.Name != "csi-driver" {
				continue
			}
			container.Env = append(container.Env, corev1.EnvVar{
				Name:  "FIPS_ENABLED",
				Value: fipsEnbaled,
			})
		}
		return nil
	}
}

func withFIPSDaemonSetHook() csidrivernodeservicecontroller.DaemonSetHookFunc {
	return withFIPSDaemonSetHookInternal(getFIPSEnabled())
}
