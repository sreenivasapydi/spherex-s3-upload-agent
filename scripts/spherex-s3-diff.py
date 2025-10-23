import sys
from pathlib import Path

sys.path.append(Path(__file__).resolve().parent.parent.as_posix())


from app.diff import run

if __name__ == "__main__":
    run()
