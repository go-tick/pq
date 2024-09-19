package pq

type ErrorListener interface {
	OnError(error)
}
