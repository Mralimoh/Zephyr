import subprocess
from pathlib import Path
import sys

def main():
    project_root = Path(__file__).resolve().parent
    go_file = project_root / "cmd" / "client"

    if not go_file.exists():
        sys.exit(1)

    try:
        subprocess.run(
            [
                "go", "run", str(go_file),
                "-c", "client_config.json",
                "-gc", "credentials.json"
            ],
            cwd=project_root,
            check=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        sys.exit(1)

if __name__ == "__main__":
    main()