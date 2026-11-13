# Seatly: Flash-Release Seat Booking on AWS

A Tatkal-style booking platform built to survive the moment a booking window opens: thousands of users, a few seats, one instant.

Seatly solves two problems that simple student booking apps ignore:

1. **Traffic spikes.** Demand can jump 50 to 100 times for a few minutes. Servers scale out for the rush and scale back in afterward.
2. **Overselling.** Two near-simultaneous requests can both read "1 seat left" and both book it. Seatly enforces correctness in the database with one atomic update.

> Course project, Oct 2026. Built and demonstrated on AWS (ap-south-1, Mumbai).
## Author
Soumya Dubey, B.Tech CSE, Ramdeobaba University, Nagpur
Harshita Chaudhari, B.Tech CSE, Ramdeobaba University, Nagpur
Meenakshi Motwani, B.Tech CSE, Ramdeobaba University, Nagpur

---

## Architecture

```
                         Internet
                            |
                 Application Load Balancer        (public subnets, 2 AZs)
                            |
              Auto Scaling Group: EC2 booking app (private subnets, 1 to 2 nodes)
               |            |                |
         RDS MySQL     S3 (e-tickets)   SSM Parameter Store
     (private subnets)  (private bucket)   (secrets, via IAM role)

        EBS log volume + snapshot  ->  point-in-time disaster recovery
        NAT Gateway (public subnet) ->  outbound-only internet for private servers
```

| Layer | AWS service | Purpose |
|---|---|---|
| Network | VPC, public and private subnets, Internet Gateway, NAT Gateway | Isolates the app and database from the internet |
| Entry | Application Load Balancer | One public address, health checks, traffic spreading |
| Compute | EC2, Launch Template, Auto Scaling Group | Stateless app nodes that scale on CPU (60% target) |
| Data | RDS MySQL | Seat inventory and bookings, atomic updates |
| Storage | S3 (private), EBS plus snapshot | E-tickets via pre-signed URLs, booking log with backup |
| Security | Security-group chain, IAM role, SSM Parameter Store | Least privilege, no hardcoded credentials |
| Monitoring | CloudWatch | CPU metrics and scaling alarms |

---

## The key idea: atomic booking

Two slots run side by side so the difference is visible:

- **NAIVE-DEMO** reads the seat count, then writes later. Under concurrent requests it **oversells** (seats go negative).
- **ATOMIC-DEMO** runs one conditional statement and checks whether a row changed:

```sql
UPDATE slots SET seats_left = seats_left - 1
WHERE id = ? AND seats_left > 0;
```

Only one request can win each seat. Under the same simultaneous load, the atomic slot ends at exactly the seat limit with **0 oversold bookings**.

---

## What was demonstrated

- **Normal booking:** book up to the limit, then the slot shows SOLD OUT and a ticket is generated.
- **Race condition:** simultaneous bookings oversell the naive slot but never the atomic one.
- **Load balancing:** one address served by two servers, with an unhealthy server removed automatically.
- **Auto Scaling:** CPU load raised a CloudWatch alarm and launched an extra server.
- **Disaster recovery:** a deleted EBS log volume was recreated from a snapshot and the booking history came back.


---

## Repository layout

```
seatly/
├── app/
│   ├── app.py              Flask booking app (naive and atomic paths, admin dashboard, demo tools)
│   └── requirements.txt
└── README.md
```

---

## How it was deployed

The AWS resources were created by hand in the **AWS Management Console**, in this order:

1. VPC (10.0.0.0/16) with 2 public and 2 private subnets across 2 Availability Zones, an Internet Gateway, a NAT Gateway and route tables.
2. Three security groups chained together: internet to load balancer, load balancer to app, app to database.
3. RDS MySQL (`db.t3.micro`, single-AZ, private, not publicly accessible).
4. Private S3 bucket for the app code and e-tickets.
5. IAM role for the servers (least privilege) and SSM Parameter Store entries for the database host and passwords.
6. Launch Template with a startup script (User Data) that installs the app and starts it.
7. Target group and Application Load Balancer, then an Auto Scaling Group (1 to 2 nodes, 60% CPU target tracking).
8. A separate EBS volume for the booking log, backed up with snapshots.

### App configuration

The app reads its settings from environment variables, and secrets from SSM Parameter Store.

| Variable | Meaning |
|---|---|
| `AWS_REGION` | Region, for example `ap-south-1` |
| `SSM_PREFIX` | Parameter path prefix, for example `/seatly` (needs `db_host`, `db_password`, `admin_password`) |
| `DB_NAME` | Database name in RDS (default `seatly`) |
| `TICKET_BUCKET` | S3 bucket for e-tickets |
| `LOG_DIR`, `LOG_MOUNT` | Booking log directory and the disk mount point it must live on |
| `DEMO_SEATS` | Seats per demo slot when the database is first created (default 3) |

---

## Run it locally

No AWS account is needed. The app falls back to SQLite:

```bash
cd app
pip install -r requirements.txt
LOG_MOUNT= LOG_DIR=./logs ADMIN_PASSWORD=changeme python app.py
```

Open `http://localhost:5000`. The admin page is at `/admin` (user `admin`), with a Reset button and demo tools for firing simultaneous bookings.

---

## Cost note

The NAT Gateway, load balancer, RDS instance and EC2 nodes bill by the hour. The project was built and demonstrated in short sessions on AWS credits, then deleted. Always delete the stack when you finish.

---

## Limitations and future work

- Single-instance RDS (a production system would use Multi-AZ failover).
- HTTP only (a production system would add HTTPS with ACM and a domain).
- Manual snapshots (a production system would schedule them).
- The booking log disk is attached to one server only. Servers added by Auto Scaling keep a temporary log, while the booking records themselves always live in RDS.
- Possible extensions: a queue (SQS) to smooth very large bursts, and a cache (ElastiCache) for seat counts at very high scale.

---
