### Data Quality Validation

def validate_data_quality(df, table_name, required_columns):
    """Validate data quality metrics"""

    # Row count check
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"{table_name}: No data found")

    # Null checks for required columns
    null_counts = {}
    for col_name in required_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percentage = (null_count / row_count) * 100

        if null_percentage > 5:  # Threshold: 5%
            raise ValueError(f"{table_name}.{col_name}: {null_percentage:.2f}% nulls exceed threshold")

        null_counts[col_name] = null_count

    print(f"{table_name} validation passed: {row_count:,} rows, null checks: {null_counts}")
    return True