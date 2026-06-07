package sqllayer

// ConsistencyMode controls whether a table uses strong (CP) or eventual (AP) consistency.
type ConsistencyMode int

const (
	ConsistencyCP ConsistencyMode = iota // strong: Raft consensus (default)
	ConsistencyAP                        // eventual: local writes, async sync
)
