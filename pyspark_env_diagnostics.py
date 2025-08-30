import sys
import os
from pathlib import Path

def diagnose_python_installation():
    """Diagnose Python installation and paths"""
    print("=== Python Installation Diagnosis ===")
    
    # Current Python executable
    print(f"Current Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    
    # Check if current executable exists
    current_python = Path(sys.executable)
    print(f"Current Python exists: {current_python.exists()}")
    
    # Expected virtual environment path
    expected_venv_python = Path("C:/Users/Dele/Documents/D. Professional Registration/IT/DATA-EnGR/00_data_engr_projects/unicargo/unicargo_dab/.venv_pyspark/Scripts/python.exe")
    print(f"Expected venv Python: {expected_venv_python}")
    print(f"Expected venv Python exists: {expected_venv_python.exists()}")
    
    # Check alternative paths
    alt_paths = [
        expected_venv_python.parent / "python3.exe",
        expected_venv_python.parent / "python3.11.exe",
        expected_venv_python.with_suffix(".EXE")  # Case sensitivity
    ]
    
    print("\n=== Alternative Python Executables ===")
    for alt_path in alt_paths:
        print(f"{alt_path}: {alt_path.exists()}")
    
    # List all files in Scripts directory
    scripts_dir = expected_venv_python.parent
    if scripts_dir.exists():
        print(f"\n=== Files in {scripts_dir} ===")
        try:
            for file in scripts_dir.iterdir():
                if file.suffix.lower() in ['.exe', '.bat']:
                    print(f"  {file.name}")
        except Exception as e:
            print(f"Error listing directory: {e}")
    
    # Environment variables
    print(f"\n=== Environment Variables ===")
    print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")
    print(f"PYSPARK_DRIVER_PYTHON: {os.environ.get('PYSPARK_DRIVER_PYTHON', 'Not set')}")
    print(f"VIRTUAL_ENV: {os.environ.get('VIRTUAL_ENV', 'Not set')}")
    print(f"PATH: {os.environ.get('PATH', 'Not set')[:200]}...")
    
    # Recommendations
    print(f"\n=== Recommendations ===")
    if not expected_venv_python.exists():
        print("❌ Virtual environment Python not found at expected location")
        print("Solutions:")
        print("1. Recreate virtual environment:")
        print("   python -m venv .venv_pyspark")
        print("   .venv_pyspark\\Scripts\\activate")
        print("   pip install pyspark")
        print("2. Or use current Python executable")
    else:
        print("✅ Virtual environment Python found")
        print("Issue might be with path escaping or environment variables")

if __name__ == "__main__":
    diagnose_python_installation()