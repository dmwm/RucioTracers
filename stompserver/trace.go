package main

import "fmt"

//Trace defines Rucio trace
type Trace struct {
	// EventVersion  default value API_1.21.6
	EventVersion string `json:"eventVersion"`
	// ClientState default value done
	ClientState string `json:"clientState"`
	// Scope  default value cms
	Scope string `json:"scope"`
	// EventType  default value get
	EventType string `json:"eventType"`
	// Usrdn default value /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fwjr/CN=1/CN=fwjr/CN=0
	Usrdn string `json:"usrdn"`
	// Account default fwjr, other options are crab, cmspop, xrootd and so on.
	Account string `json:"account"`
	// Filename defines cms LFN.
	Filename string `json:"filename"`
	// RemoteSite defines where the file was read from.
	RemoteSite string `json:"remoteSite"`
	// DID is defined as cms:lfn
	DID string `json:"DID"`
	// FileReadts defines when the file is read.
	FileReadts int64 `json:"file_read_ts"`
	// Jobtype defines the type of job.
	Jobtype string `json:"jobtype"`
	// Wnname defines the name of worknode.
	Wnname string `json:"wn_name"`
	// Timestamp defines the file read timestamp, same as FileReadts.
	Timestamp int64 `json:"timestamp"`
	// TraceTimeentryUnix defines when the trace was enteried, same as FileReadts.
	TraceTimeentryUnix int64 `json:"traceTimeentryUnix"`
}

// NewTrace creates a new instance of Rucio Trace.
func NewTrace(lfn string, site string, ts int64, jobtype string, wnname string) Trace {
	trc := Trace{
		Account:            "fwjr",
		ClientState:        "DONE",
		Filename:           lfn,
		DID:                fmt.Sprintf("cms: %s", lfn),
		EventType:          "get",
		EventVersion:       "API_1.21.6",
		FileReadts:         ts,
		RemoteSite:         site,
		Scope:              "cms",
		Timestamp:          ts,
		TraceTimeentryUnix: ts,
		Usrdn:              "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fwjr/CN=0/CN=fwjr/CN=0",
		Wnname:             wnname,
	}
	return trc
}
