package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FilterConfiguration struct {
	metav1.TypeMeta `json:",inline"`
	Packages        []Package `json:"packages"`
}

type Package struct {
	Name           string    `json:"name"`
	DefaultChannel string    `json:"defaultChannel"`
	Channels       []Channel `json:"channels"`
}

type Channel struct {
	Name         string `json:"name"`
	VersionRange string `json:"versionRange"`
}
