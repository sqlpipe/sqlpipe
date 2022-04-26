package globals

import "time"

var GitHash string
var SqlpipeVersion string
var Analytics bool
var EtcdTimeout time.Duration
var EtcdLongTimeout time.Duration
var EtcdMaxConcurrentRequests int

const LockPrefix = "sqlpipe/locks/"
