package controller

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/kubernetes-csi/external-resizer/pkg/csi"
	"github.com/kubernetes-csi/external-resizer/pkg/features"
	"github.com/kubernetes-csi/external-resizer/pkg/resizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/workqueue"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

const (
	pvcName      = "foo"
	pvcNamespace = "modify"
)

var (
	fsVolumeMode           = v1.PersistentVolumeFilesystem
	testVac                = "test-vac"
	targetVac              = "target-vac"
	infeasibleErr          = status.Errorf(codes.InvalidArgument, "Parameters in VolumeAttributesClass is invalid")
	finalErr               = status.Errorf(codes.Internal, "Final error")
	pvcConditionInProgress = v1.PersistentVolumeClaimCondition{
		Type:    v1.PersistentVolumeClaimVolumeModifyingVolume,
		Status:  v1.ConditionTrue,
		Message: "ModifyVolume operation in progress.",
	}

	pvcConditionInfeasible = v1.PersistentVolumeClaimCondition{
		Type:    v1.PersistentVolumeClaimVolumeModifyingVolume,
		Status:  v1.ConditionTrue,
		Message: "ModifyVolume failed with errorrpc error: code = InvalidArgument desc = Parameters in VolumeAttributesClass is invalid. Waiting for retry.",
	}

	pvcConditionError = v1.PersistentVolumeClaimCondition{
		Type:    v1.PersistentVolumeClaimVolumeModifyVolumeError,
		Status:  v1.ConditionTrue,
		Message: "ModifyVolume failed with error. Waiting for retry.",
	}
)

func TestMarkControllerModifyVolumeStatus(t *testing.T) {
	basePVC := makeTestPVC([]v1.PersistentVolumeClaimCondition{})

	tests := []struct {
		name               string
		pvc                *v1.PersistentVolumeClaim
		expectedPVC        *v1.PersistentVolumeClaim
		expectedConditions []v1.PersistentVolumeClaimCondition
		expectedErr        error
		testFunc           func(*v1.PersistentVolumeClaim, *resizeController) (*v1.PersistentVolumeClaim, error)
	}{
		{
			name:               "mark modify volume as in progress",
			pvc:                basePVC.get(),
			expectedPVC:        basePVC.withModifyVolumeStatus(v1.PersistentVolumeClaimModifyVolumeInProgress).get(),
			expectedConditions: []v1.PersistentVolumeClaimCondition{pvcConditionInProgress},
			expectedErr:        nil,
			testFunc: func(pvc *v1.PersistentVolumeClaim, ctrl *resizeController) (*v1.PersistentVolumeClaim, error) {
				return ctrl.markControllerModifyVolumeStatus(pvc, v1.PersistentVolumeClaimModifyVolumeInProgress, nil)
			},
		},
		{
			name:               "mark modify volume as infeasible",
			pvc:                basePVC.get(),
			expectedPVC:        basePVC.withModifyVolumeStatus(v1.PersistentVolumeClaimModifyVolumeInfeasible).get(),
			expectedConditions: []v1.PersistentVolumeClaimCondition{pvcConditionInfeasible},
			expectedErr:        infeasibleErr,
			testFunc: func(pvc *v1.PersistentVolumeClaim, ctrl *resizeController) (*v1.PersistentVolumeClaim, error) {
				return ctrl.markControllerModifyVolumeStatus(pvc, v1.PersistentVolumeClaimModifyVolumeInfeasible, infeasibleErr)
			},
		},
		{
			name:               "mark modify volume as pending",
			pvc:                basePVC.get(),
			expectedPVC:        basePVC.withModifyVolumeStatus(v1.PersistentVolumeClaimModifyVolumePending).get(),
			expectedConditions: nil,
			expectedErr:        nil,
			testFunc: func(pvc *v1.PersistentVolumeClaim, ctrl *resizeController) (*v1.PersistentVolumeClaim, error) {
				return ctrl.markControllerModifyVolumeStatus(pvc, v1.PersistentVolumeClaimModifyVolumePending, nil)
			},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, true)()
			client := csi.NewMockClient("foo", true, true, true, true)
			driverName, _ := client.GetDriverName(context.TODO())

			pvc := test.pvc

			var initialObjects []runtime.Object
			initialObjects = append(initialObjects, test.pvc)

			kubeClient, informerFactory := fakeK8s(initialObjects)

			csiResizer, err := resizer.NewResizerFromClient(client, 15*time.Second, kubeClient, informerFactory, driverName)
			if err != nil {
				t.Fatalf("Test %s: Unable to create resizer: %v", test.name, err)
			}
			controller := NewResizeController(driverName,
				csiResizer, kubeClient,
				time.Second, informerFactory,
				workqueue.DefaultControllerRateLimiter(), true /*handleVolumeInUseError*/)

			ctrlInstance, _ := controller.(*resizeController)

			pvc, err = tc.testFunc(pvc, ctrlInstance)
			if err != nil && !reflect.DeepEqual(tc.expectedErr, err) {
				t.Errorf("Expected error to be %v but got %v", tc.expectedErr, err)
			}

			realStatus := pvc.Status.ModifyVolumeStatus.Status
			expectedStatus := tc.expectedPVC.Status.ModifyVolumeStatus.Status
			if !reflect.DeepEqual(realStatus, expectedStatus) {
				t.Errorf("expected modify volume status %+v got %+v", expectedStatus, realStatus)
			}

			realConditions := pvc.Status.Conditions
			if !compareConditions(realConditions, tc.expectedConditions) {
				t.Errorf("expected conditions %+v got %+v", tc.expectedConditions, realConditions)
			}
		})
	}
}

func TestUpdateConditionBasedOnError(t *testing.T) {
	basePVC := makeTestPVC([]v1.PersistentVolumeClaimCondition{})

	tests := []struct {
		name               string
		pvc                *v1.PersistentVolumeClaim
		expectedConditions []v1.PersistentVolumeClaimCondition
		expectedErr        error
	}{
		{
			name:               "update condition based on error",
			pvc:                basePVC.get(),
			expectedConditions: []v1.PersistentVolumeClaimCondition{pvcConditionError},
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, true)()
			client := csi.NewMockClient("foo", true, true, true, true)
			driverName, _ := client.GetDriverName(context.TODO())

			pvc := test.pvc

			var initialObjects []runtime.Object
			initialObjects = append(initialObjects, test.pvc)

			kubeClient, informerFactory := fakeK8s(initialObjects)

			csiResizer, err := resizer.NewResizerFromClient(client, 15*time.Second, kubeClient, informerFactory, driverName)
			if err != nil {
				t.Fatalf("Test %s: Unable to create resizer: %v", test.name, err)
			}
			controller := NewResizeController(driverName,
				csiResizer, kubeClient,
				time.Second, informerFactory,
				workqueue.DefaultControllerRateLimiter(), true /*handleVolumeInUseError*/)

			ctrlInstance, _ := controller.(*resizeController)

			pvc, err = ctrlInstance.updateConditionBasedOnError(tc.pvc, err)
			if err != nil && !reflect.DeepEqual(tc.expectedErr, err) {
				t.Errorf("Expected error to be %v but got %v", tc.expectedErr, err)
			}

			if !compareConditions(pvc.Status.Conditions, tc.expectedConditions) {
				t.Errorf("expected conditions %+v got %+v", tc.expectedConditions, pvc.Status.Conditions)
			}
		})
	}
}

func TestMarkControllerModifyVolumeCompleted(t *testing.T) {
	basePVC := makeTestPVC([]v1.PersistentVolumeClaimCondition{})
	basePV := createTestPV(1, pvcName, pvcNamespace, "foobaz" /*pvcUID*/, &fsVolumeMode, testVac)
	expectedPV := basePV.DeepCopy()
	expectedPV.Spec.VolumeAttributesClassName = &targetVac
	expectedPVC := basePVC.withCurrentVolumeAttributesClassName(targetVac).get()

	tests := []struct {
		name        string
		pvc         *v1.PersistentVolumeClaim
		pv          *v1.PersistentVolume
		expectedPVC *v1.PersistentVolumeClaim
		expectedPV  *v1.PersistentVolume
		expectedErr error
	}{
		{
			name:        "update modify volume status to completed",
			pvc:         basePVC.get(),
			pv:          basePV,
			expectedPVC: basePVC.withCurrentVolumeAttributesClassName(targetVac).get(),
			expectedPV:  expectedPV,
		},
		{
			name:        "update modify volume status to completed, and clear conditions",
			pvc:         basePVC.withConditions([]v1.PersistentVolumeClaimCondition{pvcConditionInProgress}).get(),
			pv:          basePV,
			expectedPVC: expectedPVC,
			expectedPV:  expectedPV,
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, true)()
			client := csi.NewMockClient("foo", true, true, true, true)
			driverName, _ := client.GetDriverName(context.TODO())

			var initialObjects []runtime.Object
			initialObjects = append(initialObjects, test.pvc)
			initialObjects = append(initialObjects, test.pv)

			kubeClient, informerFactory := fakeK8s(initialObjects)

			csiResizer, err := resizer.NewResizerFromClient(client, 15*time.Second, kubeClient, informerFactory, driverName)
			if err != nil {
				t.Fatalf("Test %s: Unable to create resizer: %v", test.name, err)
			}
			controller := NewResizeController(driverName,
				csiResizer, kubeClient,
				time.Second, informerFactory,
				workqueue.DefaultControllerRateLimiter(), true /*handleVolumeInUseError*/)

			ctrlInstance, _ := controller.(*resizeController)

			actualPVC, pv, err := ctrlInstance.markControllerModifyVolumeCompleted(tc.pvc, tc.pv)
			if err != nil && !reflect.DeepEqual(tc.expectedErr, err) {
				t.Errorf("Expected error to be %v but got %v", tc.expectedErr, err)
			}

			if len(actualPVC.Status.Conditions) == 0 {
				actualPVC.Status.Conditions = []v1.PersistentVolumeClaimCondition{}
			}

			if diff := cmp.Diff(tc.expectedPVC, actualPVC); diff != "" {
				t.Errorf("expected pvc %+v got %+v, diff is: %v", tc.expectedPVC, actualPVC, diff)
			}

			if diff := cmp.Diff(tc.expectedPV, pv); diff != "" {
				t.Errorf("expected pvc %+v got %+v, diff is: %v", tc.expectedPV, pv, diff)
			}
		})
	}
}

func TestRemovePVCFromModifyVolumeUncertainCache(t *testing.T) {
	basePVC := makeTestPVC([]v1.PersistentVolumeClaimCondition{})
	basePVC.withModifyVolumeStatus(v1.PersistentVolumeClaimModifyVolumeInProgress)
	secondPVC := getTestPVC("test-vol0", "2G", "1G", "", "")
	//secondPVC.Status.ModifyVolumeStatus

	tests := []struct {
		name string
		pvc  *v1.PersistentVolumeClaim
	}{
		{
			name: "should delete the target pvc but keep the others in the cache",
			pvc:  basePVC.get(),
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			defer featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, true)()
			client := csi.NewMockClient("foo", true, true, true, true)
			driverName, _ := client.GetDriverName(context.TODO())

			var initialObjects []runtime.Object
			initialObjects = append(initialObjects, test.pvc)
			initialObjects = append(initialObjects, secondPVC)

			kubeClient, informerFactory := fakeK8s(initialObjects)
			pvInformer := informerFactory.Core().V1().PersistentVolumes()
			pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
			podInformer := informerFactory.Core().V1().Pods()

			csiResizer, err := resizer.NewResizerFromClient(client, 15*time.Second, kubeClient, informerFactory, driverName)
			if err != nil {
				t.Fatalf("Test %s: Unable to create resizer: %v", test.name, err)
			}
			controller := NewResizeController(driverName,
				csiResizer, kubeClient,
				time.Second, informerFactory,
				workqueue.DefaultControllerRateLimiter(), true /*handleVolumeInUseError*/)

			ctrlInstance, _ := controller.(*resizeController)

			stopCh := make(chan struct{})
			informerFactory.Start(stopCh)

			for _, obj := range initialObjects {
				switch obj.(type) {
				case *v1.PersistentVolume:
					pvInformer.Informer().GetStore().Add(obj)
				case *v1.PersistentVolumeClaim:
					pvcInformer.Informer().GetStore().Add(obj)
				case *v1.Pod:
					podInformer.Informer().GetStore().Add(obj)
				default:
					t.Fatalf("Test %s: Unknown initalObject type: %+v", test.name, obj)
				}
			}

			err = ctrlInstance.removePVCFromModifyVolumeUncertainCache(tc.pvc)
			if err != nil {
				t.Errorf("err deleting pvc: %v", tc.pvc)
			}

			deletedPVCKey := tc.pvc.Namespace + "/" + tc.pvc.Name
			_, exists, err := ctrlInstance.uncertainPVCs.GetByKey(deletedPVCKey)
			if exists {
				t.Errorf("pvc %v should be deleted but it is still in the uncertainPVCs cache", tc.pvc)
			}
			if err != nil {
				t.Errorf("err get pvc %v from uncertainPVCs: %v", tc.pvc, err)
			}

			notDeletedPVCKey := secondPVC.Namespace + "/" + secondPVC.Name
			_, exists, err = ctrlInstance.uncertainPVCs.GetByKey(notDeletedPVCKey)
			if !exists {
				t.Errorf("pvc %v should not be deleted", secondPVC)
			}
			if err != nil {
				t.Errorf("err get pvc %v from uncertainPVCs: %v", secondPVC, err)
			}
		})
	}
}

func createTestPV(capacityGB int, pvcName, pvcNamespace string, pvcUID types.UID, volumeMode *v1.PersistentVolumeMode, vacName string) *v1.PersistentVolume {
	capacity := quantityGB(capacityGB)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testPV",
		},
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: map[v1.ResourceName]resource.Quantity{
				v1.ResourceStorage: capacity,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "foo",
					VolumeHandle: "foo",
				},
			},
			VolumeMode:                volumeMode,
			VolumeAttributesClassName: &vacName,
		},
	}
	if len(pvcName) > 0 {
		pv.Spec.ClaimRef = &v1.ObjectReference{
			Namespace: pvcNamespace,
			Name:      pvcName,
			UID:       pvcUID,
		}
		pv.Status.Phase = v1.VolumeBound
	}
	return pv
}

func makeTestPVC(conditions []v1.PersistentVolumeClaimCondition) pvcModifier {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "modify"},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
				v1.ReadOnlyMany,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
			VolumeAttributesClassName: &targetVac,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase:      v1.ClaimBound,
			Conditions: conditions,
			Capacity: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse("2Gi"),
			},
			CurrentVolumeAttributesClassName: &testVac,
			ModifyVolumeStatus: &v1.ModifyVolumeStatus{
				TargetVolumeAttributesClassName: targetVac,
			},
		},
	}
	return pvcModifier{pvc}
}

func (m pvcModifier) withModifyVolumeStatus(status v1.PersistentVolumeClaimModifyVolumeStatus) pvcModifier {
	if m.pvc.Status.ModifyVolumeStatus == nil {
		m.pvc.Status.ModifyVolumeStatus = &v1.ModifyVolumeStatus{}
	}
	m.pvc.Status.ModifyVolumeStatus.Status = status
	return m
}

func (m pvcModifier) withCurrentVolumeAttributesClassName(currentVacName string) pvcModifier {
	if m.pvc.Status.ModifyVolumeStatus == nil {
		m.pvc.Status.ModifyVolumeStatus = &v1.ModifyVolumeStatus{}
	}
	m.pvc.Status.CurrentVolumeAttributesClassName = &currentVacName
	return m
}

func (m pvcModifier) withConditions(conditions []v1.PersistentVolumeClaimCondition) pvcModifier {
	m.pvc.Status.Conditions = conditions
	return m
}

func compareConditions(realConditions, expectedConditions []v1.PersistentVolumeClaimCondition) bool {
	if realConditions == nil && expectedConditions == nil {
		return true
	}
	if (realConditions == nil || expectedConditions == nil) || len(realConditions) != len(expectedConditions) {
		return false
	}

	for i, condition := range realConditions {
		if condition.Type != expectedConditions[i].Type || condition.Message != expectedConditions[i].Message || condition.Status != expectedConditions[i].Status {
			return false
		}
	}
	return true
}
