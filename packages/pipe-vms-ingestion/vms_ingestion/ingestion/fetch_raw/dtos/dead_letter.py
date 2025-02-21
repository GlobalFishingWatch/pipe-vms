# Dead letter
import traceback
from datetime import datetime, timezone


class DeadLetter:
    def __init__(
        self,
        message_id: str,
        attributes: dict,
        data: str,
        error: Exception,
        pipeline_step: str,
        gcs_bucket: str,
        gcs_file_path: str,
    ):
        self.message_id = message_id
        self.attributes = attributes
        self.data = data
        self.error_message = error
        self.stack_trace = "".join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__))
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.pipeline_step = pipeline_step
        self.gcs_bucket = gcs_bucket
        self.gcs_file_path = gcs_file_path

    def __str__(self):
        return f"Message: {self.message}, Error: {self.error}"

    def to_dict(self):
        return {
            "message_id": self.message_id,
            "attributes": self.attributes,
            "data": self.data,
            "error_message": str(self.error_message),
            "stack_trace": self.stack_trace,
            "timestamp": self.timestamp,
            "pipeline_step": self.pipeline_step,
            "gcs_bucket": self.gcs_bucket,
            "gcs_file_path": self.gcs_file_path,
        }
