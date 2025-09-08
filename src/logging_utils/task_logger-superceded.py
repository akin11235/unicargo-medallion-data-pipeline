import time
import uuid
import json
import psutil
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, TimestampType, IntegerType
)
from pyspark.sql.functions import current_timestamp, date_format, col

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 2
MAX_MESSAGE_LENGTH = 500
MAX_ERROR_DETAIL_LENGTH = 2000

class TaskLogger:
    """
    Enhanced task-level logging for data pipelines with better performance,
    structured data capture, and operational monitoring capabilities.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.log_schema = self._get_log_schema()
    
    def _get_log_schema(self) -> StructType:
        """Define optimized schema for task logs"""
        return StructType([
            # Core identifiers
            StructField("pipeline_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("task_id", StringType(), True),
            
            # Execution info
            StructField("operation", StringType(), True),
            StructField("status", StringType(), True),
            StructField("start_timestamp", TimestampType(), True),
            StructField("end_timestamp", TimestampType(), True),
            StructField("execution_time_ms", LongType(), True),
            
            # Data metrics
            StructField("rows_read", LongType(), True),
            StructField("rows_written", LongType(), True),
            StructField("bytes_processed", LongType(), True),
            
            # Resource usage
            StructField("memory_used_mb", DoubleType(), True),
            StructField("cpu_percent", DoubleType(), True),
            
            # Data quality
            StructField("data_quality_score", DoubleType(), True),
            StructField("null_count", LongType(), True),
            StructField("duplicate_count", LongType(), True),
            
            # Paths and references
            StructField("source_path", StringType(), True),
            StructField("target_path", StringType(), True),
            
            # Status and errors
            StructField("error_code", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("error_details", StringType(), True),
            
            # Metadata
            StructField("log_date", StringType(), True),  # Partition key
            StructField("retry_count", IntegerType(), True),
            StructField("additional_metadata", StringType(), True),  # JSON string
        ])
    
    def log_task_execution(self, 
                          status: str,
                          operation: str,
                          pipeline_name: Optional[str] = None,
                          pipeline_id: Optional[str] = None,
                          start_time: Optional[float] = None,
                          rows_read: Optional[int] = None,
                          rows_written: Optional[int] = None,
                          bytes_processed: Optional[int] = None,
                          source_path: Optional[str] = None,
                          target_path: Optional[str] = None,
                          error: Optional[Exception] = None,
                          data_quality_metrics: Optional[Dict] = None,
                          retry_count: int = 0,
                          additional_metadata: Optional[Dict] = None,
                          **kwargs) -> str:
        """
        Log task execution with comprehensive metrics and context.
        
        Args:
            status: SUCCESS, FAILED, RUNNING, SKIPPED
            operation: Specific operation name (e.g., "read_flights", "transform_customer_data")
            pipeline_name: Name of the pipeline
            pipeline_id: Unique pipeline identifier
            start_time: Unix timestamp when task started
            rows_read: Number of rows read from source
            rows_written: Number of rows written to target
            bytes_processed: Bytes of data processed
            source_path: Source data path/table
            target_path: Target data path/table
            error: Exception object if task failed
            data_quality_metrics: Dict with quality metrics
            retry_count: Number of retries attempted
            additional_metadata: Additional context as dict
            
        Returns:
            Path where log was written
        """
        
        # Get execution context
        end_time = time.time()
        execution_time_ms = int((end_time - start_time) * 1000) if start_time else None
        
        # Get system metrics
        memory_info = self._get_memory_usage()
        cpu_percent = self._get_cpu_usage()
        
        # Process error information
        error_code = None
        summary = f"{operation} {status.lower()}"
        error_details = None
        
        if error:
            error_code = type(error).__name__
            summary = f"{operation} failed: {str(error)[:100]}"
            error_details = self._format_error_details(error)
        
        # Process data quality metrics
        data_quality_score = None
        null_count = None
        duplicate_count = None
        
        if data_quality_metrics:
            data_quality_score = data_quality_metrics.get('quality_score')
            null_count = data_quality_metrics.get('null_count')
            duplicate_count = data_quality_metrics.get('duplicate_count')
        
        # Get pipeline identifiers
        pipeline_id = pipeline_id or self._get_widget("pipeline_id", str(uuid.uuid4()))
        pipeline_name = pipeline_name or self._get_widget("pipeline_name", "unknown_pipeline")
        run_id = self._get_widget("run_id", f"local_run_{int(time.time())}")
        task_id = self._get_widget("task_id", "unknown_task")
        environment = self._get_widget("catalog", "dev")
        
        # Create timestamps
        now = datetime.now(timezone.utc)
        start_timestamp = datetime.fromtimestamp(start_time, timezone.utc) if start_time else now
        
        # Create log row
        log_row = Row(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            environment=environment,
            run_id=run_id,
            task_id=task_id,
            operation=operation,
            status=status,
            start_timestamp=start_timestamp,
            end_timestamp=now,
            execution_time_ms=execution_time_ms,
            rows_read=rows_read,
            rows_written=rows_written,
            bytes_processed=bytes_processed,
            memory_used_mb=memory_info,
            cpu_percent=cpu_percent,
            data_quality_score=data_quality_score,
            null_count=null_count,
            duplicate_count=duplicate_count,
            source_path=source_path,
            target_path=target_path,
            error_code=error_code,
            summary=summary[:MAX_MESSAGE_LENGTH] if summary else None,
            error_details=error_details,
            log_date=now.strftime('%Y-%m-%d'),
            retry_count=retry_count,
            additional_metadata=json.dumps(additional_metadata) if additional_metadata else None
        )
        
        # Create DataFrame and write
        log_df = self.spark.createDataFrame([log_row], schema=self.log_schema)
        
        # Use optimized partitioning
        partition_cols = ["environment", "log_date"]
        
        return self._write_log(
            log_df, 
            log_type="task", 
            environment=environment,
            partition_cols=partition_cols,
            file_format="delta"
        )
    
    def _get_memory_usage(self) -> Optional[float]:
        """Get current memory usage in MB"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            return round(memory_info.rss / (1024 * 1024), 2)  # Convert to MB
        except Exception:
            return None
    
    def _get_cpu_usage(self) -> Optional[float]:
        """Get current CPU usage percentage"""
        try:
            return round(psutil.cpu_percent(interval=0.1), 2)
        except Exception:
            return None
    
    def _format_error_details(self, error: Exception) -> Optional[str]:
        """Format error details with stack trace"""
        try:
            error_details = {
                'error_type': type(error).__name__,
                'error_message': str(error),
                'stack_trace': traceback.format_exc()
            }
            details_json = json.dumps(error_details)
            return details_json[:MAX_ERROR_DETAIL_LENGTH]
        except Exception:
            return str(error)[:MAX_ERROR_DETAIL_LENGTH]
    
    def _get_widget(self, name: str, default: Any) -> Any:
        """Get widget value with fallback to default"""
        try:
            # Try to get from Databricks widgets
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            return dbutils.widgets.get(name)
        except Exception:
            return default
    
    def _write_log(self,
                   log_df: DataFrame,
                   log_type: str,
                   environment: str = "dev",
                   partition_cols: List[str] = None,
                   file_format: str = "delta") -> str:
        """
        Write log with retry logic and optimized partitioning
        """
        partition_cols = partition_cols or ["environment", "log_date"]
        
        # Get log path from configuration
        path = self._get_log_path(log_type, environment)
        
        # Prepare writer with optimizations
        writer = (log_df.write
                 .mode("append")
                 .partitionBy(*partition_cols)
                 .option("mergeSchema", "true")  # Handle schema evolution
                 .option("optimizeWrite", "true")  # Optimize file sizes
                 .option("autoCompact", "true"))   # Auto-compact small files
        
        # Retry logic
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if file_format.lower() == "delta":
                    writer.format("delta").save(path)
                elif file_format.lower() == "parquet":
                    writer.format("parquet").save(path)
                else:
                    raise ValueError(f"Unsupported format: {file_format}")
                
                return path  # Success
                
            except Exception as e:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"Failed to write logs after {MAX_RETRIES} attempts: {e}")
                
                print(f"Log write attempt {attempt} failed: {e}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
        
        return path
    
    def _get_log_path(self, log_type: str, environment: str) -> str:
        """Get log path from configuration"""
        base_path = f"abfss://medallion@adlsunikarrgodev.dfs.core.windows.net/logs"
        return f"{base_path}/{log_type}_logs"


# Context manager for automatic task logging
class TaskExecutionLogger:
    """
    Context manager that automatically logs task start/end with metrics capture
    
    Usage:
        with TaskExecutionLogger(logger, "read_flights", pipeline_name="data_ingestion") as task_log:
            df = spark.read.parquet("path/to/data")
            task_log.set_metrics(rows_read=df.count(), source_path="path/to/data")
            # Processing happens here
            task_log.set_metrics(rows_written=processed_df.count(), target_path="target/path")
    """
    
    def __init__(self, 
                 logger: TaskLogger, 
                 operation: str, 
                 pipeline_name: str = None,
                 **kwargs):
        self.logger = logger
        self.operation = operation
        self.pipeline_name = pipeline_name
        self.kwargs = kwargs
        self.start_time = None
        self.metrics = {}
        self.error = None
    
    def __enter__(self):
        self.start_time = time.time()
        # Log task start
        self.logger.log_task_execution(
            status="RUNNING",
            operation=self.operation,
            pipeline_name=self.pipeline_name,
            start_time=self.start_time,
            **self.kwargs
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "SUCCESS" if exc_type is None else "FAILED"
        error = exc_val if exc_type else None
        
        # Log task completion
        self.logger.log_task_execution(
            status=status,
            operation=self.operation,
            pipeline_name=self.pipeline_name,
            start_time=self.start_time,
            error=error,
            **self.metrics,
            **self.kwargs
        )
        
        return False  # Don't suppress exceptions
    
    def set_metrics(self, **metrics):
        """Update metrics during task execution"""
        self.metrics.update(metrics)


# Convenience functions
def create_task_logger(spark: SparkSession) -> TaskLogger:
    """Create a task logger instance"""
    return TaskLogger(spark)


def calculate_data_quality_metrics(df: DataFrame) -> Dict[str, Any]:
    """
    Calculate basic data quality metrics for a DataFrame
    
    Args:
        df: Spark DataFrame to analyze
        
    Returns:
        Dict with quality metrics
    """
    try:
        total_rows = df.count()
        if total_rows == 0:
            return {'quality_score': 0.0, 'null_count': 0, 'duplicate_count': 0}
        
        # Count nulls across all columns
        null_counts = []
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts.append(null_count)
        
        total_nulls = sum(null_counts)
        
        # Count duplicates (based on all columns)
        duplicate_count = total_rows - df.dropDuplicates().count()
        
        # Calculate quality score (simple metric: 1 - (nulls + duplicates) / total_cells)
        total_cells = total_rows * len(df.columns)
        problematic_cells = total_nulls + (duplicate_count * len(df.columns))
        quality_score = max(0.0, 1.0 - (problematic_cells / total_cells))
        
        return {
            'quality_score': round(quality_score, 4),
            'null_count': total_nulls,
            'duplicate_count': duplicate_count,
            'total_rows': total_rows,
            'total_columns': len(df.columns)
        }
        
    except Exception as e:
        print(f"Error calculating data quality metrics: {e}")
        return {'quality_score': None, 'null_count': None, 'duplicate_count': None}


# Example usage
def example_usage():
    """
    Example of how to use the improved logging system
    """
    spark = SparkSession.builder.appName("LoggingExample").getOrCreate()
    logger = create_task_logger(spark)
    
    # Method 1: Manual logging
    start_time = time.time()
    
    try:
        # Simulate data processing
        df = spark.range(1000).toDF("id")
        
        # Calculate quality metrics
        quality_metrics = calculate_data_quality_metrics(df)
        
        # Log success
        logger.log_task_execution(
            status="SUCCESS",
            operation="read_sample_data",
            pipeline_name="example_pipeline",
            start_time=start_time,
            rows_read=df.count(),
            source_path="memory://sample",
            data_quality_metrics=quality_metrics
        )
        
    except Exception as e:
        # Log failure
        logger.log_task_execution(
            status="FAILED",
            operation="read_sample_data",
            pipeline_name="example_pipeline",
            start_time=start_time,
            error=e
        )
        raise
    
    # Method 2: Using context manager (recommended)
    with TaskExecutionLogger(logger, "transform_data", pipeline_name="example_pipeline") as task_log:
        # Your data processing code here
        input_df = spark.range(1000).toDF("id")
        task_log.set_metrics(rows_read=input_df.count(), source_path="memory://input")
        
        # Transform data
        output_df = input_df.withColumn("doubled", col("id") * 2)
        
        # Calculate quality metrics
        quality_metrics = calculate_data_quality_metrics(output_df)
        
        task_log.set_metrics(
            rows_written=output_df.count(),
            target_path="memory://output",
            data_quality_metrics=quality_metrics
        )
        # Context manager automatically logs completion