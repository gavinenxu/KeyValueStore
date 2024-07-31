package bitcask_go

var transactionFinishKey = []byte("transaction_finish_key")

var sequenceNumberKey = []byte("sequence_number_key")

const nonTransactionSequenceNumber uint64 = 0

const initialDataFileId uint32 = 1

const (
	mergeDirNameSuffix = "-merge"
	mergeFinishKey     = "merge_finish_key"
)
