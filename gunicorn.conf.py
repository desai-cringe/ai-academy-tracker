#!/usr/bin/env python3
"""
Gunicorn Configuration for Memory-Optimized AI Academy Tracker
Prevents worker timeouts and memory issues with large datasets
"""

# Server socket
bind = "127.0.0.1:8000"
backlog = 2048

# Worker processes
workers = 1  # Single worker to prevent memory issues
worker_class = "gthread"
worker_connections = 1000
threads = 4  # Multiple threads instead of workers

# Worker lifecycle
max_requests = 100  # Restart worker after 100 requests to prevent memory leaks
max_requests_jitter = 20  # Add randomness to prevent all workers restarting at once
preload_app = True  # Load app before forking workers
timeout = 600  # 10 minutes timeout for large file processing
keepalive = 2

# Memory management
worker_tmp_dir = "/dev/shm"  # Use RAM disk for temporary files
limit_request_line = 8190
limit_request_fields = 100
limit_request_field_size = 8190

# Logging
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'
accesslog = "-"  # Log to stdout
errorlog = "-"  # Log to stderr

# Process naming
proc_name = "ai-academy-tracker"

# Graceful timeout
graceful_timeout = 120

# Security
forwarded_allow_ips = "*"

# Environment variables for large file handling
raw_env = [
    "PYTHONUNBUFFERED=1",
    "MALLOC_ARENA_MAX=2",  # Limit malloc arenas to reduce memory fragmentation
    "PYTHONGC=1",  # Enable garbage collection
]

def when_ready(server):
    """Called just after the server is started."""
    print("🚀 AI Academy Tracker server is ready!")

def worker_int(worker):
    """Called just after a worker exited on SIGINT or SIGQUIT."""
    print(f"⚠️ Worker {worker.pid} received interrupt signal")

def pre_fork(server, worker):
    """Called just before a worker is forked."""
    print(f"🔧 Forking worker {worker.age}")

def post_fork(server, worker):
    """Called just after a worker has been forked."""
    print(f"✅ Worker {worker.pid} spawned")

def worker_abort(worker):
    """Called when a worker received the SIGABRT signal."""
    print(f"❌ Worker {worker.pid} aborted")

# Memory monitoring
def on_starting(server):
    """Called just before the master process is initialized."""
    print("🔧 Starting AI Academy Tracker with memory optimizations...")
    print(f"   Workers: {workers}")
    print(f"   Threads per worker: {threads}")
    print(f"   Timeout: {timeout}s")
    print(f"   Max requests per worker: {max_requests}")

def on_reload(server):
    """Called to recycle workers during a reload via SIGHUP."""
    print("🔄 Reloading workers...")

def on_exit(server):
    """Called just before exiting."""
    print("👋 AI Academy Tracker shutting down...")

# Performance tuning for large datasets
def post_worker_init(worker):
    """Called just after a worker has initialized the application."""
    import gc
    # Enable garbage collection optimizations
    gc.set_threshold(700, 10, 10)  # More aggressive garbage collection
    print(f"🧹 Worker {worker.pid} initialized with optimized garbage collection")
