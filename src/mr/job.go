package mr

type MapJob struct {
	FileName     string
	MapJobNumber int
	Reducers     int

	ID int
}

type ReduceJob struct {
	IntermediateFiles []string
	ReduceId          int

	ID int
}
