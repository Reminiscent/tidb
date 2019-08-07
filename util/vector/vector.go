package vector

// Vector represents a vector of values.
//type Vector unsafe.Pointer

type Vec interface {
	Length() int
	Get(index int) (x interface{}, exists bool)
	Set(index int, x interface{}, exists bool)
}

type NullBitMap struct {
	data []bool
}

func (nbm *NullBitMap) get(index int) bool {
	return nbm.data[index]
}

func (nbm *NullBitMap) set(index int, value bool) {
	nbm.data[index] = value
}

// VecInt64 represents a vector of int64 values.
type VecInt64 struct {
	data []int64
	nbm  NullBitMap
}

func NewVecInt64(length int) *VecInt64 {
	return &VecInt64{
		data: make([]int64, length),
		nbm:  NullBitMap{make([]bool, length)},
	}
}

func (v *VecInt64) Length() int {
	return len(v.data)
}

func (v *VecInt64) Get(index int) (x interface{}, exists bool) {
	if v.nbm.get(index) {
		return v.data[index], true
	}
	return 0, false
}

func (v *VecInt64) Set(index int, x interface{}, exists bool) {
	if exists {
		v.data[index] = x.(int64)
	}
	v.nbm.set(index, exists)
}
