# Unikargo ETL & Table Configuration Pipeline

```mermaid

┌───────────────────────┐
│       tables.yaml      │
│-----------------------│
│ airlines:             │
│   bronze: "unikargo…" │
│   silver: "unikargo…" │
│   gold:               │
│     daily_perf: "…"   │
│ airports:             │
│   bronze: …           │
│   silver: …           │
│   gold: …             │
│ flights:              │
│   bronze: …           │
│   silver: …           │
│   gold: …             │
└─────────┬─────────────┘
          │ loaded by
          ▼
┌───────────────────────────────┐
│   TableConfig / get_table_config │
│---------------------------------│
│ Inputs: entity, layer,          │
│         environment, table_key  │
│ Processes:                     │
│ 1. Look up ENVIRONMENTS[env]   │
│ 2. Map logical layer → schema   │
│ 3. Lookup table name in YAML   │
│ Output: TableConfig instance    │
│   - catalog                     │
│   - schema (prefixed)           │
│   - table                       │
│   - layer (logical)             │
│   - table_key (optional)        │
└─────────┬─────────────────────┘
          │ used by
          ▼
┌───────────────────────────────┐
│       save_to_table(df, …)     │
│--------------------------------│
│ Inputs: DataFrame, TableConfig  │
│ Processes:                     │
│ 1. df.write.mode(mode)         │
│ 2. Apply overwriteSchema if set │
│ 3. Save as table: full_name     │
│ Output: Data saved in catalog.schema.table
└─────────┬──────────────┘
          │
          ▼
┌───────────────────────────────┐
│   Convenience wrappers        │
│-------------------------------│
│ save_to_bronze(df, entity)    │
│ save_to_silver(df, entity)    │
│ save_to_gold(df, entity, key) │
│  └─ internally calls save_to_table
└───────────────────────────────┘


Here’s a clear summary of what you’ve achieved so far in your Unikargo data engineering setup:

---

### **1️⃣ Table configuration and YAML design**

* Defined all tables in a **single `tables.yaml`** with separate layers (`bronze`, `silver`, `gold`) and logical names.
* Gold tables have **sub-keys** (e.g., `daily_summary`, `cancellation_rates`) to allow multiple aggregate tables per entity.
* YAML is now flexible: no numeric prefixes in the YAML, making it easier to maintain in the future.

---

### **2️⃣ Environment configuration**

* Defined `ENVIRONMENTS` dictionary for `dev`, `staging`, `prod`.
* Each environment specifies:

  * Catalog name (`unikargo_dev`, `unikargo_staging`, `unikargo_prod`).
  * Schema prefixes (`01_bronze`, `02_silver`, `03_gold`) for physical table names.
  * Checkpoint locations for streaming.

---

### **3️⃣ `TableConfig` dataclass**

* Encapsulates table information: catalog, schema (prefixed), table, layer (logical), optional table\_key.
* Provides `full_name` property for fully qualified table name (`catalog.schema.table`).
* Keeps logical layer (`bronze`, `silver`, `gold`) separate from physical schema name (`01_bronze`, `02_silver`, `03_gold`).

---

### **4️⃣ `get_table_config` function**

* Resolves **entity + layer + environment + table\_key** to a `TableConfig` instance.
* Automatically maps **logical layer → numeric schema** using `prefix_map` in `ENVIRONMENTS`.
* Supports gold-layer sub-tables via `table_key`.

---

### **5️⃣ Save functions**

* **Generic:** `save_to_table(df, entity, layer, environment, table_key, ...)`

  * Handles writing any DataFrame to the correct table in any layer.
* **Convenience wrappers:**

  * `save_to_bronze(df, entity)`
  * `save_to_silver(df, entity)`
  * `save_to_gold(df, entity, table_key)`
  * These reduce mistakes with layer and table\_key parameters.
* Support for **mode** (`overwrite`, `append`) and **overwrite\_schema** via `**kwargs`.

---

### **6️⃣ Benefits achieved**

* Fully **dynamic table resolution** based on environment and layer.
* **Single source of truth** for table names in YAML.
* Clear separation of **logical layer names** and **physical schema prefixes**.
* Convenience and safety when saving DataFrames, reducing risk of overwriting the wrong table.
* Future-proof: YAML changes (like removing numeric prefixes) won’t break code; schema prefixes can be centrally managed in `ENVIRONMENTS`.

---



