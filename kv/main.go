package kv

type Store struct {
	db map[string]string
}

func NewStore() *Store {
	store := &Store{db: map[string]string{}}
	return store
}

func (k Store) Get(key string) string {
	return k.db[key]
}

func (k Store) Put(key string, value string) {
	k.db[key] = value
}
