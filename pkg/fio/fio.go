package fio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	kankube "github.com/kanisterio/kanister/pkg/kube"
	"github.com/kastenhq/kubestr/pkg/common"
	gerrors "github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	sv1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	// DefaultNS describes the default namespace
	DefaultNS = "default"
	// PodNamespaceEnvKey describes the pod namespace env variable
	PodNamespaceEnvKey = "POD_NAMESPACE"
	// DefaultFIOJob describes the default FIO job
	DefaultFIOJob = "default-fio"
	// KubestrFIOJobGenName describes the generate name
	KubestrFIOJobGenName = "kubestr-fio"
	// ConfigMapJobKey is the default fio job key
	ConfigMapJobKey = "fiojob"
	// DefaultPVCSize is the default PVC size
	DefaultPVCSize = "100Gi"
	// PVCGenerateName is the name to generate for the PVC
	PVCGenerateName = "kubestr-fio-pvc-"
	// PodGenerateName is the name to generate for the POD
	PodGenerateName = "kubestr-fio-pod-"
	// ContainerName is the name of the container that runs the job
	ContainerName = "kubestr-fio"
	// PodNameEnvKey is the name of the variable used to get the current pod name
	PodNameEnvKey = "HOSTNAME"
	// ConfigMapMountPath is the path where we mount the configmap
	ConfigMapMountPath = "/etc/fio-config"
	// VolumeMountPath is the path where we mount the volume
	VolumeMountPath = "/dataset"
	// CreatedByFIOLabel is the key that desrcibes the label used to mark configmaps
	CreatedByFIOLabel = "createdbyfio"
)

// FIO is an interface that represents FIO related commands
type FIO interface {
	RunFio(ctx context.Context, args *RunFIOArgs) (*RunFIOResult, error) // , test config
}

// FIOrunner implments FIO
type FIOrunner struct {
	Cli      kubernetes.Interface
	fioSteps fioSteps
	Pods     []*v1.Pod
	PVCs     []*v1.PersistentVolumeClaim
	StorageClass *sv1.StorageClass
	ConfigMap  *v1.ConfigMap
	TestFileName   string
}

type RunFIOArgs struct {
	StorageClass   string
	Size           string
	Namespace      string
	NodeSelector   map[string]string
	FIOJobFilepath string
	FIOJobName     string
	Image          string
	NumPods        int
}

func (a *RunFIOArgs) Validate() error {
	if a.StorageClass == "" || a.Size == "" || a.Namespace == "" {
		return fmt.Errorf("Require fields are missing. (StorageClass, Size, Namespace)")
	}
	return nil
}

type RunFIOResult struct {
	Size         string            `json:"size,omitempty"`
	StorageClass *sv1.StorageClass `json:"storageClass,omitempty"`
	FioConfig    string            `json:"fioConfig,omitempty"`
	// Maps Pod name to FioResult.
	Result       map[string]FioResult         `json:"result,omitempty"`
}

func (f *FIOrunner) CreateResources(ctx context.Context, args *RunFIOArgs) error {
	f.fioSteps = &fioStepper{
		cli:          f.Cli,
		podReady:     &podReadyChecker{cli: f.Cli},
		kubeExecutor: &kubeExecutor{cli: f.Cli},
	}

	// create a configmap with test parameters
	if f.Cli == nil { // for UT purposes
		return fmt.Errorf("cli uninitialized")
	}

	if err := args.Validate(); err != nil {
		return err
	}

	if err := f.fioSteps.validateNamespace(ctx, args.Namespace); err != nil {
		return gerrors.Wrapf(err, "Unable to find namespace (%s)", args.Namespace)
	}

	if err := f.fioSteps.validateNodeSelector(ctx, args.NodeSelector); err != nil {
		return gerrors.Wrapf(err, "Unable to find nodes satisfying node selector (%v)", args.NodeSelector)
	}

	configMap, err := f.fioSteps.loadConfigMap(ctx, args)
	if err != nil {
		return gerrors.Wrap(err, "Unable to create a ConfigMap")
	}

	f.TestFileName, err = fioTestFilename(configMap.Data)
	if err != nil {
		return gerrors.Wrap(err, "Failed to get test file name.")
	}

	fmt.Printf("Creating %d PVCs and Pods\n", args.NumPods)
	for i := 1; i <= args.NumPods; i++ {
		pvc, err := f.fioSteps.createPVC(ctx, args.StorageClass, args.Size, args.Namespace)
		if err != nil {
		   return gerrors.Wrap(err, "Failed to create PVC")
		}
		f.PVCs = append(f.PVCs, pvc)
		fmt.Println("PVC created", pvc.Name)

	        pod, err := f.fioSteps.createPod(ctx, pvc.Name, configMap.Name, f.TestFileName, args.Namespace, args.NodeSelector, args.Image)
	        if err != nil {
		   return gerrors.Wrap(err, "Failed to create POD")
		}
		f.Pods = append(f.Pods, pod) 
		fmt.Println("Pod created", pod.Name)
	}
	return err
}

func (f *FIOrunner) RunFio(ctx context.Context, args *RunFIOArgs) (*RunFIOResult, error) {
	var err error
	if f.StorageClass == nil {
	   f.StorageClass, err = f.fioSteps.storageClassExists(ctx, args.StorageClass)
	   if err != nil {
		return nil, gerrors.Wrap(err, "Cannot find StorageClass")
	   }
	}
	if f.ConfigMap == nil {
	   f.ConfigMap, err = f.fioSteps.loadConfigMap(ctx, args)
	   if err != nil {
		return nil, gerrors.Wrap(err, "Unable to create a ConfigMap")
	   }
	}

	fmt.Printf("Running FIO test (%s) on StorageClass (%s) with %d PVCs of Size (%s)\n", f.TestFileName, args.StorageClass, args.NumPods, args.Size)
	fioOutput, err := f.fioSteps.runFIOCommand(ctx, f.Pods, ContainerName, f.TestFileName, args.Namespace)
	if err != nil {
		return nil, gerrors.Wrap(err, "Failed while running FIO test.")
	}
	return &RunFIOResult{
		Size:         args.Size,
		StorageClass: f.StorageClass,
		FioConfig:    f.ConfigMap.Data[f.TestFileName],
		Result:       fioOutput,
	}, nil
}

type fioSteps interface {
	validateNamespace(ctx context.Context, namespace string) error
	validateNodeSelector(ctx context.Context, selector map[string]string) error
	storageClassExists(ctx context.Context, storageClass string) (*sv1.StorageClass, error)
	loadConfigMap(ctx context.Context, args *RunFIOArgs) (*v1.ConfigMap, error)
	createPVC(ctx context.Context, storageclass, size, namespace string) (*v1.PersistentVolumeClaim, error)
	deletePVC(ctx context.Context, pvcName, namespace string) error
	createPod(ctx context.Context, pvcName, configMapName, testFileName, namespace string, nodeSelector map[string]string, image string) (*v1.Pod, error)
	deletePod(ctx context.Context, podName, namespace string) error
	runFIOCommand(ctx context.Context, pods []*v1.Pod, containerName, testFileName, namespace string) (map[string]FioResult, error)
	deleteConfigMap(ctx context.Context, configMap *v1.ConfigMap, namespace string) error
}

type fioStepper struct {
	cli          kubernetes.Interface
	podReady     waitForPodReadyInterface
	kubeExecutor kubeExecInterface
}

func (s *fioStepper) validateNamespace(ctx context.Context, namespace string) error {
	if _, err := s.cli.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{}); err != nil {
		return err
	}
	return nil
}

func (s *fioStepper) validateNodeSelector(ctx context.Context, selector map[string]string) error {
	nodes, err := s.cli.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(selector).String(),
	})
	if err != nil {
		return err
	}

	if len(nodes.Items) == 0 {
		return fmt.Errorf("No nodes match selector")
	}

	return nil
}

func (s *fioStepper) storageClassExists(ctx context.Context, storageClass string) (*sv1.StorageClass, error) {
	return s.cli.StorageV1().StorageClasses().Get(ctx, storageClass, metav1.GetOptions{})
}

func (s *fioStepper) loadConfigMap(ctx context.Context, args *RunFIOArgs) (*v1.ConfigMap, error) {
	configMap := &v1.ConfigMap{
		Data: make(map[string]string),
	}
	switch {
	case args.FIOJobFilepath != "":
		data, err := os.ReadFile(args.FIOJobFilepath)
		if err != nil {
			return nil, gerrors.Wrap(err, "File reading error")
		}
		configMap.Data[filepath.Base(args.FIOJobFilepath)] = string(data)
	case args.FIOJobName != "":
		if _, ok := fioJobs[args.FIOJobName]; !ok {
			return nil, fmt.Errorf("FIO job not found- (%s)", args.FIOJobName)
		}
		configMap.Data[args.FIOJobName] = fioJobs[args.FIOJobName]
	default:
		configMap.Data[DefaultFIOJob] = fioJobs[DefaultFIOJob]
	}
	// create
	configMap.GenerateName = KubestrFIOJobGenName
	configMap.Labels = map[string]string{CreatedByFIOLabel: "true"}
	return s.cli.CoreV1().ConfigMaps(args.Namespace).Create(ctx, configMap, metav1.CreateOptions{})
}

func (s *fioStepper) createPVC(ctx context.Context, storageclass, size, namespace string) (*v1.PersistentVolumeClaim, error) {
	sizeResource, err := resource.ParseQuantity(size)
	if err != nil {
		return nil, gerrors.Wrapf(err, "Unable to parse PVC size (%s)", size)
	}
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: PVCGenerateName,
			Annotations: map[string]string{
			    "robin.io/replication": "3",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: &storageclass,
			AccessModes:      []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): sizeResource,
				},
			},
		},
	}
	return s.cli.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
}

func (s *fioStepper) deletePVC(ctx context.Context, pvcName, namespace string) error {
	return s.cli.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{})
}

func (s *fioStepper) createPod(ctx context.Context, pvcName, configMapName, testFileName, namespace string, nodeSelector map[string]string, image string) (*v1.Pod, error) {
	if pvcName == "" || configMapName == "" || testFileName == "" {
		return nil, fmt.Errorf("Create pod missing required arguments.")
	}

	if image == "" {
		image = common.DefaultPodImage
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: PodGenerateName,
			Namespace:    namespace,
		},
		Spec: v1.PodSpec{
		        TopologySpreadConstraints: []v1.TopologySpreadConstraint{{
			    MaxSkew: int32(1),
			    WhenUnsatisfiable: v1.DoNotSchedule,
			    TopologyKey: "kubernetes.io/hostname",
			}},
			Containers: []v1.Container{{
				Name:    ContainerName,
				Command: []string{"/bin/sh"},
				Args:    []string{"-c", "tail -f /dev/null"},
				VolumeMounts: []v1.VolumeMount{
					{Name: "persistent-storage", MountPath: VolumeMountPath},
					{Name: "config-map", MountPath: ConfigMapMountPath},
				},
				Image: image,
			}},
			Volumes: []v1.Volume{
				{
					Name: "persistent-storage",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
					},
				},
				{
					Name: "config-map",
					VolumeSource: v1.VolumeSource{
						ConfigMap: &v1.ConfigMapVolumeSource{
							LocalObjectReference: v1.LocalObjectReference{
								Name: configMapName,
							},
						},
					},
				},
			},
			NodeSelector: nodeSelector,
		},
	}
	podRes, err := s.cli.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return podRes, err
	}

	err = s.podReady.waitForPodReady(ctx, namespace, podRes.Name)
	if err != nil {
		return nil, err
	}

	podRes, err = s.cli.CoreV1().Pods(namespace).Get(ctx, podRes.Name, metav1.GetOptions{})
	if err != nil {
		return podRes, err
	}

	return podRes, nil
}

func (s *fioStepper) deletePod(ctx context.Context, podName, namespace string) error {
	return s.cli.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
}

func (s *fioStepper) runFIOCommand(ctx context.Context, pods []*v1.Pod, containerName, testFileName, namespace string) (map[string]FioResult, error) {
	jobFilePath := fmt.Sprintf("%s/%s", ConfigMapMountPath, testFileName)
	command := []string{"fio", "--directory", VolumeMountPath, jobFilePath, "--output-format=json"}

	fioResults := make(map[string]FioResult, len(pods))
	lock := sync.RWMutex{}

	var err error
	var execErrCount int
	timestart := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < len(pods); i++ {
	    wg.Add(1)
	    i := i
	    go func() {
	        defer wg.Done()
		stdout, stderr, err := s.kubeExecutor.exec(namespace, pods[i].Name, containerName, command)
		thisErrCount := 0
		if err != nil {
		   thisErrCount++
		}
		var fioOut FioResult
		err = json.Unmarshal([]byte(stdout), &fioOut)
		if err != nil {
		   err = gerrors.Wrapf(err, "Unable to parse fio output into json.")
		   thisErrCount++
		}
		lock.Lock()
		fioResults[pods[i].Name] = fioOut
		execErrCount += thisErrCount
		lock.Unlock()
		if err != nil || stderr != "" {
			if err == nil {
				err = errors.Join(err, fmt.Errorf("stderr when running FIO"))
			}
			err = gerrors.Wrapf(err, "Error running command:(%v), stderr:(%s)", command, stderr)
		}
	    }()
	}
	fmt.Println("Waiting for exec Pods to finish")
	wg.Wait()
	fmt.Println("-----------------------------------------------------\n\n")
        fmt.Printf("Start time: %s; elapsed time: %s num errors of random RW in Pods: %d\n", timestart.Format("2006-01-02 15:04:05"), time.Since(timestart), execErrCount)
	if err != nil {
		return fioResults, err
	}
	return fioResults, nil
}

// deleteConfigMap only deletes a config map if it has the label
func (s *fioStepper) deleteConfigMap(ctx context.Context, configMap *v1.ConfigMap, namespace string) error {
	if val, ok := configMap.Labels[CreatedByFIOLabel]; ok && val == "true" {
		return s.cli.CoreV1().ConfigMaps(namespace).Delete(ctx, configMap.Name, metav1.DeleteOptions{})
	}
	return nil
}

func fioTestFilename(configMap map[string]string) (string, error) {
	if len(configMap) != 1 {
		return "", fmt.Errorf("Unable to find fio file in configmap/more than one found %v", configMap)
	}
	var fileName string
	for key := range configMap {
		fileName = key
	}
	return fileName, nil
}

type waitForPodReadyInterface interface {
	waitForPodReady(ctx context.Context, namespace string, name string) error
}

type podReadyChecker struct {
	cli kubernetes.Interface
}

func (p *podReadyChecker) waitForPodReady(ctx context.Context, namespace, name string) error {
	return kankube.WaitForPodReady(ctx, p.cli, namespace, name)
}

type kubeExecInterface interface {
	exec(namespace, podName, containerName string, command []string) (string, string, error)
}

type kubeExecutor struct {
	cli kubernetes.Interface
}

func (k *kubeExecutor) exec(namespace, podName, containerName string, command []string) (string, string, error) {
	return kankube.Exec(k.cli, namespace, podName, containerName, command, nil)
}
