package crate

import (
	"context"
	"reflect"

	cratev1alpha1 "github.com/gree-gorey/crate-operator/pkg/apis/crate/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_crate")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new Crate Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileCrate{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("crate-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource Crate
	err = c.Watch(&source.Kind{Type: &cratev1alpha1.Crate{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner Crate
	err = c.Watch(&source.Kind{Type: &appsv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &cratev1alpha1.Crate{},
	})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileCrate{}

// ReconcileCrate reconciles a Crate object
type ReconcileCrate struct {
	// TODO: Clarify the split client
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a Crate object and makes changes based on the state read
// and what is in the Crate.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Crate StatefulSet for each Crate CR
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCrate) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling Crate")

	// Fetch the Crate instance
	crate := &cratev1alpha1.Crate{}
	err := r.client.Get(context.TODO(), request.NamespacedName, crate)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			reqLogger.Info("Crate resource not found. Ignoring since object must be deleted")
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		reqLogger.Error(err, "Failed to get Crate")
		return reconcile.Result{}, err
	}

	// Check if the StatefulSet already exists, if not create a new one
	found := &appsv1.StatefulSet{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: crate.Name, Namespace: crate.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		// Define a new StatefulSet
		st := r.statefulSetForCrate(crate)
		reqLogger.Info("Creating a new StatefulSet", "StatefulSet.Namespace", st.Namespace, "StatefulSet.Name", st.Name)
		err = r.client.Create(context.TODO(), st)
		if err != nil {
			reqLogger.Error(err, "Failed to create new StatefulSet", "StatefulSet.Namespace", st.Namespace, "StatefulSet.Name", st.Name)
			return reconcile.Result{}, err
		}
		// StatefulSet created successfully - return and requeue
		return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get StatefulSet")
		return reconcile.Result{}, err
	}

	// Ensure the StatefulSet size is the same as the spec
	size := crate.Spec.Size
	if *found.Spec.Replicas != size {
		found.Spec.Replicas = &size
		err = r.client.Update(context.TODO(), found)
		if err != nil {
			reqLogger.Error(err, "Failed to update StatefulSet", "StatefulSet.Namespace", found.Namespace, "StatefulSet.Name", found.Name)
			return reconcile.Result{}, err
		}
		// Spec updated - return and requeue
		return reconcile.Result{Requeue: true}, nil
	}

	// Update the Crate status with the pod names
	// List the pods for this crate's StatefulSet
	podList := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(labelsForCrate(crate.Name))
	listOps := &client.ListOptions{Namespace: crate.Namespace, LabelSelector: labelSelector}
	err = r.client.List(context.TODO(), listOps, podList)
	if err != nil {
		reqLogger.Error(err, "Failed to list pods", "Crate.Namespace", crate.Namespace, "Crate.Name", crate.Name)
		return reconcile.Result{}, err
	}
	podNames := getPodNames(podList.Items)

	// Update status.Nodes if needed
	if !reflect.DeepEqual(podNames, crate.Status.Nodes) {
		crate.Status.Nodes = podNames
		err := r.client.Status().Update(context.TODO(), crate)
		if err != nil {
			reqLogger.Error(err, "Failed to update Crate status")
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

// statefulSetForCrate returns a crate StatefulSet object
func (r *ReconcileCrate) statefulSetForCrate(m *cratev1alpha1.Crate) *appsv1.StatefulSet {
	ls := labelsForCrate(m.Name)
	replicas := m.Spec.Size

	st := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image:   "nginx",
						Name:    "crate",
						Ports: []corev1.ContainerPort{{
							ContainerPort: 80,
							Name:          "http",
						}},
					}},
				},
			},
		},
	}
	// Set Crate instance as the owner and controller
	controllerutil.SetControllerReference(m, st, r.scheme)
	return st
}

// labelsForCrate returns the labels for selecting the resources
// belonging to the given crate CR name.
func labelsForCrate(name string) map[string]string {
	return map[string]string{"app": "crate", "crate_cr": name}
}

// getPodNames returns the pod names of the array of pods passed in
func getPodNames(pods []corev1.Pod) []string {
	var podNames []string
	for _, pod := range pods {
		podNames = append(podNames, pod.Name)
	}
	return podNames
}
