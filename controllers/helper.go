/*
2025 NVIDIA CORPORATION & AFFILIATES

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
package controllers

import (
	constants "github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/consts"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type DrainAnnotationPredicate struct {
	predicate.Funcs
}

func (DrainAnnotationPredicate) Create(e event.CreateEvent) bool {
	if e.Object == nil {
		return false
	}

	if _, hasAnno := e.Object.GetAnnotations()[constants.NodeDrainAnnotation]; hasAnno {
		return true
	}
	return false
}

func (DrainAnnotationPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		return false
	}
	if e.ObjectNew == nil {
		return false
	}

	oldAnno, hasOldAnno := e.ObjectOld.GetAnnotations()[constants.NodeDrainAnnotation]
	newAnno, hasNewAnno := e.ObjectNew.GetAnnotations()[constants.NodeDrainAnnotation]

	if !hasOldAnno && hasNewAnno {
		return true
	}

	return oldAnno != newAnno
}

type DrainStateAnnotationPredicate struct {
	predicate.Funcs
}

func (DrainStateAnnotationPredicate) Create(e event.CreateEvent) bool {
	return e.Object != nil
}

func (DrainStateAnnotationPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		return false
	}
	if e.ObjectNew == nil {
		return false
	}

	oldAnno, hasOldAnno := e.ObjectOld.GetLabels()[constants.NodeStateDrainAnnotationCurrent]
	newAnno, hasNewAnno := e.ObjectNew.GetLabels()[constants.NodeStateDrainAnnotationCurrent]

	if !hasOldAnno || !hasNewAnno {
		return true
	}

	return oldAnno != newAnno
}
