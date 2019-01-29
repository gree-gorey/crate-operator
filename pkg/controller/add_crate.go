package controller

import (
	"github.com/gree-gorey/crate-operator/pkg/controller/crate"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, crate.Add)
}
