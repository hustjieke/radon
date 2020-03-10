package shift

type ShiftHandler interface {
	Start() error
	GetCanalStatus() bool
	GetCanalStatus() bool
	SetCanalStatus(b bool)
	ChecksumTable() error
}
