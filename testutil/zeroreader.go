package testutil

type ZeroReader struct{}

func (ZeroReader) Read(b []byte) (n int, err error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}
