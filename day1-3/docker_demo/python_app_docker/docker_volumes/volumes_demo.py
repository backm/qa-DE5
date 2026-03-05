from pathlib import Path
from datetime import datetime

out = Path("data/hello.txt")

with out.open("a") as f:
    f.write(f"Hello to the docker volume! ((datetime.now()))\n")

print("File written")
