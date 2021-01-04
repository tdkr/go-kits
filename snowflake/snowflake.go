package snowflake

import "time"

type ID int64

type Node struct {
	id    int64
	step  int64
	epoch time.Time
	time  int64

	nodeMask  int64
	nodeShift uint8
	timeShift uint8
	stepMask  int64
}

func NewNode(id int64) *Node {
	n := &Node{
		id: id,
	}
	return n
}

func (n *Node) Generate() ID {
	now := time.Since(n.epoch).Nanoseconds() / 1000000

	if now == n.time {
		n.step = (n.step + 1) & n.stepMask
		if n.step == 0 {
			for now <= n.time {
				now = time.Since(n.epoch).Nanoseconds() / 1000000
			}
		}
	}

	n.time = now

	id := (n.time << n.timeShift) |
		(n.id << n.nodeShift) |
		n.step

	return ID(id)
}
