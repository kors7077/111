from dataclasses import dataclass
from typing import Optional

@dataclass
class Flight:
    id: Optional[int] = None
    price: float = 0.0
    plane: str = ""