// Package causality ensures causal consistency in the key-value store.
// We implement explicit causal dependency lists to track causal dependencies.
package causality

// WaitForDependencies stalls a replica that needs to
// receive messages with causally dependent versions before
// it stores its current message.
// Maybe have some queue for each replica?
func WaitForDependencies() {

}

// CheckDependencyHistory will search through a replica's
// locally stored metadata in order to determine whether
// or not it has causually dependent versions in its dependency
// list. If yes, you may store the new message with an updated
// version number. If not, maybem wait for dependencies?
func CheckDependencyHistory() {

}
