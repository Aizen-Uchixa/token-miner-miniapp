from dataclasses import dataclass


@dataclass
class MeResponse:
    user_id: int
    username: str
    coins: int
    gems: int
    prestige_points: int
    active_mine: str
    company: dict

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "coins": self.coins,
            "gems": self.gems,
            "prestige_points": self.prestige_points,
            "active_mine": self.active_mine,
            "company": self.company,
        }
