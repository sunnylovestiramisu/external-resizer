/*
Copyright 2023 The Kubernetes Authors.

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

package controller

import (
	"fmt"

	"github.com/kubernetes-csi/external-resizer/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	storagev1alpha1 "k8s.io/api/storage/v1alpha1"
	"k8s.io/klog/v2"
)

// The return value bool is only used as a sentinel value when function returns without actually performing modification
func (ctrl *resizeController) modify(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) (*v1.PersistentVolumeClaim, *v1.PersistentVolume, error, bool) {
	pvcSpecVacName := pvc.Spec.VolumeAttributesClassName
	curVacName := pvc.Status.CurrentVolumeAttributesClassName
	targetVacName := pvc.Status.ModifyVolumeStatus.TargetVolumeAttributesClassName

	if pvcSpecVacName != nil && curVacName != nil && *pvcSpecVacName != *curVacName {
		if *curVacName == targetVacName {
			return ctrl.validateVACAndModifyVolumeWithTarget(pvc, pv)
		} else {
			// Check if the PVC is in uncertain State
			_, exists, err := ctrl.uncertainPVCs.GetByKey(pvc.Name)
			if err != nil {
				return pvc, pv, err, false
			}

			if !exists {
				// PVC is not in uncertain state
				klog.V(3).InfoS("PVC is deleted or does not exist")
				return ctrl.validateVACAndModifyVolumeWithTarget(pvc, pv)
			} else {
				vacObj, _, err := ctrl.volumeAttributesClasses.GetByKey(*pvcSpecVacName)
				if err != nil {
					return pvc, pv, err, false
				}
				return ctrl.controllerModifyVolumeWithTarget(pvc, pv, vacObj.(*storagev1alpha1.VolumeAttributesClass), pvcSpecVacName)
			}

		}

	}
	// No modification required
	return pvc, pv, nil, false
}

// func validateVACAndModifyVolumeWithTarget validate the VAC. The function sets pvc.Status.ModifyVolumeStatus
// to Pending if VAC does not exist and proceeds to trigger ModifyVolume if VAC exists
func (ctrl *resizeController) validateVACAndModifyVolumeWithTarget(
	pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume) (*v1.PersistentVolumeClaim, *v1.PersistentVolume, error, bool) {
	// The controller only triggers ModifyVolume if pvcSpecVacName is not nil nor empty
	pvcSpecVacName := pvc.Spec.VolumeAttributesClassName
	// Check if pvcSpecVac is valid and exist
	vacObj, exists, err := ctrl.volumeAttributesClasses.GetByKey(*pvcSpecVacName)
	if exists && err == nil {
		// Mark pvc.Status.ModifyVolumeStatus as in progress
		pvc, err = ctrl.markControllerModifyVolumeStatus(pvc, v1.PersistentVolumeClaimModifyVolumeInProgress, nil)
		// Record an event to indicate that external resizer is modifying this volume.
		ctrl.eventRecorder.Event(pvc, v1.EventTypeNormal, util.VolumeModify,
			fmt.Sprintf("external resizer is modifying volume %s", pvc.Name))
		return ctrl.controllerModifyVolumeWithTarget(pvc, pv, vacObj.(*storagev1alpha1.VolumeAttributesClass), pvcSpecVacName)
	} else {
		// Mark pvc.Status.ModifyVolumeStatus as pending
		pvc, err = ctrl.markControllerModifyVolumeStatus(pvc, v1.PersistentVolumeClaimModifyVolumePending, nil)
		return pvc, pv, nil, false
	}
}

// func controllerModifyVolumeWithTarget trigger the CSI ControllerModifyVolume API call
// and handle both success and error scenarios
func (ctrl *resizeController) controllerModifyVolumeWithTarget(
	pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume,
	vacObj *storagev1alpha1.VolumeAttributesClass,
	pvcSpecVacName *string) (*v1.PersistentVolumeClaim, *v1.PersistentVolume, error, bool) {
	var err error
	pvc, pv, err = ctrl.callModifyVolumeOnPlugin(pvc, pv, vacObj)
	if err == nil {
		klog.V(4).Infof("Update volumeAttributesClass of PV %q to %s succeeded", pv.Name, *pvcSpecVacName)
		return pvc, pv, nil, true
	} else {
		if status, ok := status.FromError(err); ok && status.Code() == codes.InvalidArgument {
			// Mark pvc.Status.ModifyVolumeStatus as infeasible
			pvc, err = ctrl.markControllerModifyVolumeStatus(pvc, v1.PersistentVolumeClaimModifyVolumeInfeasible, err)
		} else {
			ctrl.updateConditionBasedOnError(pvc, err)
			if !isFinalError(err) {
				// update conditions and cache pvc as uncertain
				ctrl.uncertainPVCs.Add(pvc)
			} else {
				ctrl.removePVCFromModifyVolumeUncertainCache(pvc)
			}
		}
		// Record an event to indicate that resize operation is failed.
		ctrl.eventRecorder.Eventf(pvc, v1.EventTypeWarning, util.VolumeModifyFailed, err.Error())
		return pvc, pv, err, true
	}
}

func (ctrl *resizeController) callModifyVolumeOnPlugin(
	pvc *v1.PersistentVolumeClaim,
	pv *v1.PersistentVolume,
	vac *storagev1alpha1.VolumeAttributesClass) (*v1.PersistentVolumeClaim, *v1.PersistentVolume, error) {
	err := ctrl.resizer.Modify(pv, vac.Parameters)

	if err != nil && isFinalError(err) {
		// Mark pvc.Status.ModifyVolumeStatus as infeasible
		pvc, markModifyVolumeInfeasibleError := ctrl.markControllerModifyVolumeStatus(pvc, v1.PersistentVolumeClaimModifyVolumeInfeasible, err)
		if markModifyVolumeInfeasibleError != nil {
			return pvc, pv, fmt.Errorf("modify volume failed in controller with %v but failed to update PVC %s with: %v", err, pvc.Name, markModifyVolumeInfeasibleError)
		}
	}

	if err != nil {
		return pvc, pv, err
	}

	pvc, pv, err = ctrl.markControllerModifyVolumeCompleted(pvc, pv)
	return pvc, pv, nil
}
