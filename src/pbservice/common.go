package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	Primary	= "Primary"
	Backup = "Backup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	RequestId int64
	RequestType string


}

type PutAppendReply struct {
	Err Err
	PreviousValue string // For PutHash

}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64
	RequestType string


}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.

type TransferDataArgs struct {
	Data map[string]string
}

type TransferDataReply struct {
	Err Err
	Data map[string]string
	BackUpDone bool

}
