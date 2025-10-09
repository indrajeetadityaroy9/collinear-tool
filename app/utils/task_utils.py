import hashlib
import json
import logging
from functools import wraps
from celery import Task, current_task
from app.integrations.redis_client import get_redis_sync
log = logging.getLogger(__name__)

def generate_task_key(task_name, args, kwargs):
    serializable_args = []
    for arg in args:
        if hasattr(arg, 'request') and hasattr(arg, 'apply_async'):
            continue
        try:
            json.dumps(arg)
            serializable_args.append(arg)
        except (TypeError, ValueError):
            serializable_args.append(str(arg))

    serializable_kwargs = {}
    for key, value in kwargs.items():
        try:
            json.dumps(value)
            serializable_kwargs[key] = value
        except (TypeError, ValueError):
            serializable_kwargs[key] = str(value)

    task_data = {
        'name': task_name,
        'args': serializable_args,
        'kwargs': serializable_kwargs
    }
    task_string = json.dumps(task_data, sort_keys=True)
    task_hash = hashlib.md5(task_string.encode()).hexdigest()
    return f"task:dedup:{task_name}:{task_hash}"

def deduplicate_task(timeout=300):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            redis_client = get_redis_sync()
            if not redis_client:
                return func(*args, **kwargs)
            task_name = current_task.name if current_task else func.__name__
            task_key = generate_task_key(task_name, args, kwargs)
            existing_task_id = redis_client.get(task_key)
            if existing_task_id:
                log.info(f"Task {task_name} is already running or was recently completed. Task ID: {existing_task_id}")
                return {'status': 'deduplicated', 'existing_task_id': existing_task_id.decode() if isinstance(existing_task_id, bytes) else existing_task_id}
            task_id = current_task.request.id if current_task else 'direct-call'
            redis_client.setex(task_key, timeout, task_id)
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as exc:
                redis_client.delete(task_key)
                raise
        return wrapper
    return decorator

class ProgressTracker:
    def __init__(self, total_items, task_id=None):
        self.total_items = total_items
        self.current_item = 0
        self.task_id = task_id or (current_task.request.id if current_task else None)
        self.redis_client = get_redis_sync()
        self.progress_key = f"task:progress:{self.task_id}" if self.task_id else None

    def update(self, current=None, message=None):
        if current is not None:
            self.current_item = current
        else:
            self.current_item += 1
        if self.redis_client and self.progress_key:
            progress_data = {
                'current': self.current_item,
                'total': self.total_items,
                'percentage': round((self.current_item / self.total_items) * 100, 2) if self.total_items > 0 else 0,
                'message': message or f"Processing item {self.current_item} of {self.total_items}"
            }
            self.redis_client.setex(
                self.progress_key,
                3600,
                json.dumps(progress_data)
            )
        if current_task:
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'current': self.current_item,
                    'total': self.total_items,
                    'message': message
                }
            )

    def complete(self, message=None):
        self.current_item = self.total_items
        self.update(message=message or "Task completed successfully")
        if self.redis_client and self.progress_key:
            self.redis_client.delete(self.progress_key)
            
def get_task_progress(task_id):
    redis_client = get_redis_sync()
    if not redis_client:
        return None
    progress_key = f"task:progress:{task_id}"
    progress_data = redis_client.get(progress_key)
    if progress_data:
        try:
            return json.loads(progress_data)
        except json.JSONDecodeError:
            return None
    return None