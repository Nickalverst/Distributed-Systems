from dataclasses import dataclass, asdict
from typing import Optional
import json
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey

# 1. Base Class (Fields shared by all events)
#Sent by Promotion (every published promotion) and Ranking (everytime a promotion is featured) 
@dataclass
class BaseEvent:
    promotion_id: str
    category: str
    product_name: str = "Unknown Product"
    store_email: str = ""
    signature: Optional[str] = None 

    def _generate_stable_payload(self) -> bytes:
        """Generates a byte representation of the event to create the hash."""
        data = asdict(self)
        data.pop('signature', None)
        json_string = json.dumps(data, sort_keys=True)
        return json_string.encode('utf-8')

    def sign_event(self, private_key: RSAPrivateKey) -> None:
        """Signs the event with the private key."""
        payload = self._generate_stable_payload()
        signature_bytes = private_key.sign(
            payload,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        self.signature = base64.b64encode(signature_bytes).decode('utf-8')

    def is_signature_valid(self, public_key: RSAPublicKey) -> bool:
        """Validates the event signature."""
        if not self.signature:
            return False
        payload = self._generate_stable_payload()
        signature_bytes = base64.b64decode(self.signature.encode('utf-8'))
        try:
            public_key.verify(
                signature_bytes,
                payload,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False

# 2. Specific Events (Clear Contracts)

#Sent by Gateway when a new promotion is created 
@dataclass
class PromotionReceivedEvent(BaseEvent):
    product_name: str = "Unknown Product"

#Sent by Promotion when a promotion is published (after receiving valid signature from store) and is received by the gateway 
@dataclass
class PromotionPublishedEvent(BaseEvent):
    pass

#Sent by Gateway when a promotion is voted on by a user. The vote can be +1 (upvote) or -1 (downvote).
@dataclass
class VoteEvent(BaseEvent):
    vote: int = 0 # +1 or -1
    total_active_users: int = 0

#Sent by notification microservice when a promotion is featured or published 
@dataclass
class NotificationEvent(BaseEvent):
    is_hot_deal: bool = False 

# 3. Usage example and JSON serialization
if __name__ == "__main__":
    vote_event = VoteEvent(promotion_id="123-abc", vote=1, category="game")

    payload_json = json.dumps(asdict(vote_event))
    print(f"Sending: {payload_json}")
    # Output: Sending: {"promotion_id": "123-abc", "Signature": null, "vote": 1, "category": "game"}