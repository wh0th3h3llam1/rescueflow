package platform

func (s *Store) LockForAPI()   { s.mu.Lock() }
func (s *Store) UnlockForAPI() { s.mu.Unlock() }
