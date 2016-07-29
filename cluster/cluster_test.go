package cluster

import (
	"fmt"
	"sync"
	"time"
)

// This example joins a sole node cluster, and shows how to watch
// cluster changes and trigger transitions.
func ExampleCluster_output() {

	c, err := NewCluster()
	if err != nil {
		fmt.Printf("Error creating cluster: %v\n", err)
	}

	if err = c.Join([]string{}); err != nil {
		fmt.Printf("Error joining cluster: %v\n", err)
	}

	clusterChgCh := c.NotifyClusterChanges()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, ok := <-clusterChgCh
			if !ok {
				return
			}

			fmt.Printf("A cluster change occurred, running a transition.\n")
			if err := c.Transition(1 * time.Second); err != nil {
				fmt.Printf("Transition error: %v", err)
			}
		}
	}()

	// Leave the cluster (this triggers a cluster change event)
	c.Leave(1 * time.Second)

	// This will cause the goroutine to exit
	close(clusterChgCh)
	wg.Wait()

	// Output: A cluster change occurred, running a transition.
}
