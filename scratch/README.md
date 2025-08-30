# scratch

This folder is reserved for personal, exploratory notebooks.
By default these are not committed to Git, as 'scratch' is listed in .gitignore.


src/
├── citibike/           # Domain-specific logic
│   ├── citibike_utils.py
│   └── __init__.py
└── utils/              # General utilities
    ├── datetime_utils.py
    └── __init__.py


src/
├── streaming/          # All streaming operations
├── data_quality/       # Data cleaning and validation
├── transformations/    # Data transformations
├── io/                 # I/O operations
├── config/             # Configuration management
└── utils/              # General utilities

src/
├── streaming/
│   ├── __init__.py
│   ├── readers.py          # Streaming read operations
│   ├── writers.py          # Streaming write operations
│   └── checkpoints.py      # Checkpoint management
├── data_quality/
│   ├── __init__.py
│   ├── cleaning.py         # Data cleaning functions
│   ├── validation.py       # Data validation functions
│   └── deduplication.py    # Duplicate handling
├── transformations/
│   ├── __init__.py
│   ├── column_ops.py       # Column operations (add, drop, rename)
│   ├── null_handling.py    # Null value operations
│   └── aggregations.py     # Data aggregation functions
├── io/
│   ├── __init__.py
│   ├── delta_ops.py        # Delta table operations
│   ├── table_utils.py      # General table utilities
│   └── catalog_utils.py    # Unity Catalog operations
├── config/
│   ├── __init__.py
│   ├── table_config.py     # Table configurations
│   └── constants.py        # Application constants
└── utils/
    ├── __init__.py
    ├── logging_utils.py     # Logging utilities
    └── datetime_utils.py    # Date/time operations