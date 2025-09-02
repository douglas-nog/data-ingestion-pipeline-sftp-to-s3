import logging
import json
import sys
import uuid
from datetime import datetime
from typing import Optional

class JsonFormatter(logging.Formatter):
    
    _std_attrs = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message"
    }
    
    def format(self, record: logging.LogRecord) -> str:
        
        message = record.getMessage()
        
        ts = datetime.utcfromtimestamp(record.created).isoformat(timespec="milliseconds") + "Z"
        
        log_record = {
            "level": record.levelname,
            "message": message,
            "time": ts,
            "logger": record.name,
        }
        
        extras = {k: v for k, v in record.__dict__.items() if k not in self._std_attrs}
        if extras:
            log_record.update(extras)

        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
            
        return json.dumps(log_record, ensure_ascii=False)
    
def get_logger(name: str = "pipeline", level: str = "INFO", run_id: Optional[str] = None) -> logging.LoggerAdapter:
    
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    
    base_logger = logging.getLogger(name)
    base_logger.setLevel(numeric_level)
    
    if not base_logger.handlers:
        base_logger.addHandler(handler)

    base_logger.propagate = False
    
    context = {"run_id": run_id or str(uuid.uuid4())}
    
    return logging.LoggerAdapter(base_logger, extra=context)
