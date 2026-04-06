# CloudDrive — Distributed File Storage System

A fault-tolerant distributed file storage system built as a university
group assignment. Demonstrates fault tolerance, data replication,
time synchronization, and consensus across a 4-node simulated cluster.

---

## Team Members

| Member | Registration No. | Name | Component |
|------|-----------------|-------|-----------|
| Member 1 | IT24103456 | B.G.R.V Senanayake | Fault Tolerance |
| Member 2 | IT24103458 | T.H.B Nizam | Replication & Consistency |
| Member 3 | IT24103428 | A.M.P.V.T.C Abeyweera | Time Synchronization |
| Member 4 | IT24103440 | K.A.C.V Kasthuriarachchi | Consensus & Agreement |

---

## System Architecture

```
Clients (curl / browser)
        |
   API Gateway (Flask — port 6001-6004)
        |
   ┌────┴─────────────────────────────┐
   |  Member 1   Member 2   Member 3   Member 4  |
   |  Fault      Replica-   Time       Consensus  |
   |  Tolerance  tion Mgr   Sync       (Raft)     |
   └────┬─────────────────────────────┘
        |
   Distributed Storage Cluster
   node_A:5001  node_B:5002  node_C:5003  node_D:5004
```

---

## How to Run

### Prerequisites
- Python 3.10 or higher
- 4 terminal windows

### Step 1 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 2 — Start all 4 nodes (one terminal each)
```bash
# Terminal 1
python -m node.run --id node_A --port 5001

# Terminal 2
python -m node.run --id node_B --port 5002

# Terminal 3
python -m node.run --id node_C --port 5003

# Terminal 4
python -m node.run --id node_D --port 5004
```

Each node's HTTP API is available at port = node_port + 1000:
- node_A API: http://127.0.0.1:6001
- node_B API: http://127.0.0.1:6002
- node_C API: http://127.0.0.1:6003
- node_D API: http://127.0.0.1:6004

### Step 3 — Upload a file
```bash
curl -X POST http://127.0.0.1:6001/upload \
     -H "Content-Type: application/json" \
     -d '{"filename": "hello.txt", "content": "Hello CloudDrive!"}'
```

### Step 4 — Download from any node
```bash
curl http://127.0.0.1:6003/download/hello.txt
```

### Step 5 — Check cluster status
```bash
curl http://127.0.0.1:6001/status
```

---

## Demo Scenarios

### Demo 1 — Replication
```bash
# Upload a file
curl -X POST http://127.0.0.1:6001/upload \
     -H "Content-Type: application/json" \
     -d '{"filename": "test.txt", "content": "Replicated!"}'

# Verify it's on all 4 nodes
curl http://127.0.0.1:6001/cluster/sync-check/test.txt
```

### Demo 2 — Fault Tolerance
```bash
# Close terminal 2 (kills node_B)
# Upload while node_B is down
curl -X POST http://127.0.0.1:6001/upload \
     -H "Content-Type: application/json" \
     -d '{"filename": "while_down.txt", "content": "Written while B was down"}'

# Restart node_B in terminal 2
python -m node.run --id node_B --port 5002

# Trigger recovery
curl -X POST http://127.0.0.1:6002/simulate/recover

# Verify node_B has the file
curl http://127.0.0.1:6002/files
```

### Demo 3 — Leader Election
```bash
# See current leader
curl http://127.0.0.1:6001/status | python -m json.tool

# Kill the leader (close its terminal)
# Watch other terminals — a new election fires within 5-10 seconds
# Check new leader
curl http://127.0.0.1:6002/status | python -m json.tool
```

### Demo 4 — Clock Skew
```bash
# Run the NTP demo script
python time_sync/ntp_client.py
```

---

## Running Tests
```bash
# All tests
pytest tests/ -v

# Individual member tests
pytest tests/test_fault.py       -v   # Member 1
pytest tests/test_replication.py -v   # Member 2
pytest tests/test_timesync.py    -v   # Member 3
pytest tests/test_consensus.py   -v   # Member 4
```

---

## Project Structure

```
clouddrive-distributed-system/
├── node/                         # Shared base (all members)
│   ├── node.py                   # Base StorageNode class
│   ├── config.py                 # Cluster configuration
│   └── run.py                    # Node startup script
├── fault_tolerance/              # Member 1
│   ├── heartbeat.py
│   ├── failure_detector.py
│   └── recovery.py
├── replication/                  # Member 2
│   ├── replication_manager.py
│   ├── conflict_resolver.py
│   └── consistency.py
├── time_sync/                    # Member 3
│   ├── lamport_clock.py
│   ├── vector_clock.py
│   └── ntp_client.py
├── consensus/                    # Member 4
│   ├── raft.py
│   ├── leader_election.py
│   └── log_manager.py
├── api/
│   └── server.py                 # Flask HTTP API (shared)
├── tests/
│   ├── test_fault.py
│   ├── test_replication.py
│   ├── test_timesync.py
│   └── test_consensus.py
├── requirements.txt
└── README.md
```

---

## Links
- GitHub: [https://github.com/Viduna-Abeyweera/clouddrive-distributed-system.git]
- Presentation Video: []
