package replication

import (
	"fmt"

	btree "github.com/your-username/DistributedDatabaseSystem/internal/bTree"
	pb "github.com/your-username/DistributedDatabaseSystem/proto/repl"
)

type ReplicationServer struct {
	pb.UnimplementedReplicationServiceServer
	rm *ReplicationManager
}

func NewReplicationServer(rm *ReplicationManager) *ReplicationServer {
	return &ReplicationServer{rm: rm}
}

func fieldToProto(f btree.Field) (*pb.FieldValue, error) {
	switch v := f.Value.(type) {
	case btree.IntValue:
		return &pb.FieldValue{Value: &pb.FieldValue_IntValue{IntValue: v.V}}, nil
	case btree.StringValue:
		return &pb.FieldValue{Value: &pb.FieldValue_StringValue{StringValue: v.V}}, nil
	case btree.NullValue:
		return &pb.FieldValue{}, nil
	case btree.ListValue:
		elems := make([]*pb.FieldValue, len(v.Elems))
		for i, e := range v.Elems {
			pv, err := fieldToProto(btree.Field{Tag: 0, Value: e})
			if err != nil {
				return nil, fmt.Errorf("list element %d (tag %d): %w", i, f.Tag, err)
			}
			elems[i] = pv
		}
		return &pb.FieldValue{Value: &pb.FieldValue_ListValue{
			ListValue: &pb.FieldList{ElemType: uint32(v.ElemType), Elems: elems},
		}}, nil
	default:
		return nil, fmt.Errorf("unknown field value type (tag %d)", f.Tag)
	}
}

func (rs *ReplicationServer) StreamUpdates(req *pb.PullRequest, stream pb.ReplicationService_StreamUpdatesServer) error {
	lastSent := req.StartLsn
	ctx := stream.Context()

	// When the follower disconnects, broadcast so cond.Wait returns immediately
	// instead of blocking the goroutine until the next log write (goroutine-leak fix).
	go func() {
		<-ctx.Done()
		rs.rm.cond.Broadcast()
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}

		// Hold the lock across the read and the wait so that appendOne cannot
		// broadcast between our "no entries" check and cond.Wait (lost-wakeup fix).
		rs.rm.mu.Lock()
		entries, err := rs.rm.readFromLocked(lastSent)
		if err != nil {
			rs.rm.mu.Unlock()
			return fmt.Errorf("StreamUpdates: read from replication log: %w", err)
		}
		if len(entries) == 0 {
			rs.rm.cond.Wait()
			rs.rm.mu.Unlock()
			continue
		}
		rs.rm.mu.Unlock()

		pbEntries := make([]*pb.ReplicationLogEntry, len(entries))
		for i, entry := range entries {
			fields := make([]*pb.FieldValue, len(entry.Fields))
			for j, f := range entry.Fields {
				fields[j], err = fieldToProto(f)
				if err != nil {
					return fmt.Errorf("StreamUpdates: %w", err)
				}
			}
			pbEntries[i] = &pb.ReplicationLogEntry{
				Lsn:    entry.LSN,
				Op:     int32(entry.Op),
				Key:    entry.Key,
				Fields: fields,
			}
			lastSent = entry.LSN + 1
		}
		if err := stream.Send(&pb.PullResponse{Entries: pbEntries}); err != nil {
			return fmt.Errorf("StreamUpdates: send to client: %w", err)
		}
	}
}
