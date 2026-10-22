// Package openapi embeds the Swagger document that `make swagger` generates
// here with swag from the annotations in internal/api, so the binary serves
// the document it was built with. Do not edit swagger.json or swagger.yaml.
package openapi

import _ "embed"

var (
	//go:embed swagger.json
	JSON []byte
	//go:embed swagger.yaml
	YAML []byte
)
