package zkelection

import (
	"github.com/samuel/go-zookeeper/zk"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestElection(t *testing.T) {
	testElection(t, []int{0, 1, 2}, []int{0, 1, 2})
	testElection(t, []int{1, 0, 2}, []int{0, 2})
	testElection(t, []int{0, 2, 1}, []int{0, 1})
}

func testElection(t *testing.T, closeSequence []int, leaderSequence []int) {
	servers := []string{"localhost:2181"}
	electionPath := "/test/election"

	zkConn, _, err := zk.Connect(servers, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	zkConn.Delete(electionPath, -1)
	var cans []*Candidate
	var stopWaitChs []chan bool
	t.Log("creating candidates")

	for i := 0; i < 3; i++ {
		can, err := NewCandidate(servers, time.Second, electionPath, strconv.Itoa(i), nil)

		if err != nil {
			t.Fatal(err)
		}
		cans = append(cans, can)
		stop := make(chan bool, 1)
		stopWaitChs = append(stopWaitChs, stop)
	}
	for i := 0; i < 3; i++ {
		isLeader, _, value, err := cans[i].ElectionStatus()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if !isLeader {
				t.Error("first candidate should be leader")
			}
		} else {
			if isLeader {
				t.Error("other candidates should not be leader")
			}
		}

		if value != "0" {
			t.Error("leader value should be 0")
		}
	}

	actualLeaderSequence := []int{0}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cans[1].WaitToBeElected(stopWaitChs[1])
		if err != nil {
			return
		}
		isLeader, _, value, err := cans[1].ElectionStatus()
		if err != nil {
			t.Fatal(err)
		}
		if !isLeader || value != "1" {
			t.Error("can1 should be leader now")
			return
		}
		actualLeaderSequence = append(actualLeaderSequence, 1)
		t.Log("can2 is leader")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cans[2].WaitToBeElected(stopWaitChs[2])
		if err != nil {
			return
		}
		isLeader, _, value, err := cans[2].ElectionStatus()
		if err != nil {
			t.Fatal(err)
		}
		if !isLeader || value != "2" {
			t.Error("can2 should be leader now")
			return
		}
		actualLeaderSequence = append(actualLeaderSequence, 2)
		t.Log("can3 is leader")
	}()
	t.Log("begin")
	for _, closeConn := range closeSequence {
		time.Sleep(100 * time.Millisecond)
		cans[closeConn].Close()
		stopWaitChs[closeConn] <- true
		_, _, _, err := cans[closeConn].ElectionStatus()
		if err != ErrClosed {
			t.Error("election status should return ErrClosed after close.")
		}
		t.Log("conn closed", closeConn)
	}
	wg.Wait()
	if !reflect.DeepEqual(leaderSequence, actualLeaderSequence) {
		t.Error("expects leader sequence ", leaderSequence, "got", actualLeaderSequence)
	}
}
