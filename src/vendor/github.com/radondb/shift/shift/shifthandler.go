package shift

type ShiftHandler interface {
	// Start used to start a shift work.
	Start() error

	// WaitFinish used to wait success or fail signal to finish.
	WaitFinish() error

	// GetCanalStatus used to get status when canal running.
	GetCanalStatus() bool

	// SetCanalStatus used to set if canal is running with exception or not.
	SetCanalStatus(b bool)

	// ChecksumTable use to checksum data src tbl and dst tbl.
	ChecksumTable() error
}
