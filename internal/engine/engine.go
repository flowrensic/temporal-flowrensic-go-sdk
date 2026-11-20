// Package engine is what the calls of this SDK say about it and about
// Temporal.
package engine

import "go.flowrensic.com/temporal-flowrensic-go-sdk/internal/transport"

// TemporalSdkHeader carries the version of the Temporal Go SDK.
const TemporalSdkHeader = "Flowrensic-Temporal-Sdk"

const modulePath = "go.flowrensic.com/temporal-flowrensic-go-sdk"

const temporalModulePath = "go.temporal.io/sdk"

// SdkHeaderValue is go, a slash and the version of this SDK: the tag of the
// module in the build info of the program, dev when the module has no version
// there or is replaced by a folder.
var SdkHeaderValue = "go/" + transport.SdkVersion(modulePath)

// TemporalSdkHeaderValue is the version of go.temporal.io/sdk in the build
// info of the program, with no leading v, empty when it is not known.
var TemporalSdkHeaderValue = temporalSdkVersion()

func temporalSdkVersion() string {
	v := transport.ModuleVersion(temporalModulePath)
	if v == "(devel)" {
		return ""
	}
	return v
}

// Tag is what every call of this SDK carries about it and about Temporal.
func Tag() transport.Tag {
	return transport.Tag{Sdk: SdkHeaderValue, EngineHeader: TemporalSdkHeader, EngineValue: TemporalSdkHeaderValue}
}
